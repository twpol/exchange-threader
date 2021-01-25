using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Exchange.WebServices.Data;
using Microsoft.Extensions.Configuration;

namespace Exchange_Threader
{
    class Program
    {
        // Specification: https://docs.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxprops/57f8de0f-5f53-423a-8947-7943dd959997
        static ExtendedPropertyDefinition pidTagConversationIndex = new ExtendedPropertyDefinition(0x0071, MapiPropertyType.Binary);

        /// <param name="folder">Exchange folder to scan for conversations to fix</param>
        /// <param name="config">Path to configuration file</param>
        /// <param name="dryRun">Do not perform any actions, only pretend</param>
        static void Main(string folder, FileInfo config = null, bool dryRun = false)
        {
            if (config == null) config = new FileInfo("config.json");
            if (String.IsNullOrEmpty(folder)) throw new InvalidOperationException("Must specify Exchange folder to scan");
            if (dryRun) Console.WriteLine("DRY RUN mode - no changes will be saved");
            RunAsync(LoadConfiguration(config), dryRun, folder).Wait();
        }

        static IConfigurationRoot LoadConfiguration(FileInfo config)
        {
            return new ConfigurationBuilder()
                .AddJsonFile(config.FullName, true)
                .Build();
        }

        static async System.Threading.Tasks.Task RunAsync(IConfigurationRoot config, bool dryRun, string folderPath)
        {
            var service = new ExchangeService(ExchangeVersion.Exchange2016);
            service.Credentials = new WebCredentials(config["username"], config["password"]);
            service.AutodiscoverUrl(config["email"], redirectionUri => new Uri(redirectionUri).Scheme == "https");

            Console.WriteLine($"Looking for folder '{folderPath}'...");
            var folder = await GetFolder(service, folderPath);

            Console.WriteLine($"Looking for conversations...");
            var conversations = await Retry("find conversations", () => service.FindConversation(new ConversationIndexedItemView(int.MaxValue), folder.Id));
            var conversationTopics = conversations.Select(c => c.Topic).Distinct();

            var emailsUpdated = 0;
            foreach (var conversationTopic in conversationTopics)
            {
                emailsUpdated += await FixEmailConversationTopic(service, conversationTopic, dryRun);
            }

            if (dryRun)
            {
                Console.WriteLine($"Would have updated {emailsUpdated} emails");
            }
            else
            {
                Console.WriteLine($"Updated {emailsUpdated} emails");
            }
        }

        static async System.Threading.Tasks.Task<Folder> GetFolder(ExchangeService service, string path)
        {
            return await GetChildFolder(await Retry("bind root folder", () => Folder.Bind(service, WellKnownFolderName.MsgFolderRoot)), path.Split("/").ToArray());
        }

        static async System.Threading.Tasks.Task<Folder> GetChildFolder(Folder folder, string[] children)
        {
            var folders = await Retry("find folder", () => folder.FindFolders(new SearchFilter.IsEqualTo(FolderSchema.DisplayName, children[0]), new FolderView(10)));
            if (folders.Folders.Count != 1) throw new InvalidDataException($"Cannot find folder level '{children[0]}' below '{folder.DisplayName}'");
            if (children.Length > 1)
            {
                return await Retry("find child folder", () => GetChildFolder(folders.Folders[0], children.Skip(1).ToArray()));
            }
            return folders.Folders[0];
        }

        static async System.Threading.Tasks.Task<int> FixEmailConversationTopic(ExchangeService service, string conversationTopic, bool dryRun)
        {
            if (String.IsNullOrWhiteSpace(conversationTopic)) return 0;
            Console.WriteLine(conversationTopic);

            // Find Outlook's own search folder "AllItems", which includes all folders in the account.
            var allItems = await Retry("find AllItems folder", () => service.FindFolders(WellKnownFolderName.Root, new SearchFilter.IsEqualTo(FolderSchema.DisplayName, "AllItems"), new FolderView(10)));
            if (allItems.Folders.Count != 1) throw new MissingMemberException("AllItems");

            // Find all emails in this conversation
            var emailFilter = new SearchFilter.SearchFilterCollection(LogicalOperator.And)
            {
                new SearchFilter.IsEqualTo(ItemSchema.ItemClass, "IPM.Note"),
                new SearchFilter.IsEqualTo(EmailMessageSchema.ConversationTopic, conversationTopic),
            };
            var emailView = new ItemView(1000)
            {
                PropertySet = new PropertySet(BasePropertySet.IdOnly, ItemSchema.DateTimeSent, ItemSchema.DateTimeReceived, EmailMessageSchema.ConversationIndex, EmailMessageSchema.InternetMessageId, EmailMessageSchema.InReplyTo, EmailMessageSchema.References),
            };

            List<EmailMessage> emails = new();
            FindItemsResults<Item> all;
            do
            {
                all = await Retry("find conversation items", () => allItems.Folders[0].FindItems(emailFilter, emailView));
                emails.AddRange(all.Items.Cast<EmailMessage>());
                emailView.Offset = all.NextPageOffset ?? 0;
            } while (all.MoreAvailable);

            foreach (var email in emails.OrderBy(email => email.DateTimeSent))
            {
                if (String.IsNullOrWhiteSpace(email.InReplyTo) && !String.IsNullOrWhiteSpace(email.References))
                {
                    Console.WriteLine($"WARNING: Email with references only: {email.DateTimeSent} - {email.InternetMessageId} <-- {email.InReplyTo} / {email.References}");
                }
            }

            var threads = new List<ThreadedEmailMessage>();
            var allThreads = new Dictionary<string, ThreadedEmailMessage>();
            var messageIds = new HashSet<string>(emails.Select(email => email.InternetMessageId));

            while (emails.Count > 0)
            {
                var matched = 0;
                foreach (var email in emails.OrderBy(email => email.DateTimeSent).ToArray())
                {
                    if (email.InReplyTo != null && allThreads.ContainsKey(email.InReplyTo))
                    {
                        // Child of an existing message
                        var thread = new ThreadedEmailMessage(email);
                        allThreads[email.InternetMessageId] = thread;
                        allThreads[email.InReplyTo].AddChild(thread);
                        emails.Remove(email);
                        matched++;
                    }
                    else if (email.InReplyTo != null && !messageIds.Contains(email.InReplyTo))
                    {
                        // Child of a message we don't have - fake the parent
                        Console.WriteLine($"WARNING: Email In-Reply-To {email.InReplyTo} not found; faking it");
                        var thread = new ThreadedEmailMessage(email);
                        allThreads[email.InternetMessageId] = thread;
                        allThreads[email.InReplyTo] = thread;
                        threads.Add(thread);
                        emails.Remove(email);
                        matched++;
                    }
                    else if (email.InReplyTo == null)
                    {
                        // Root message
                        var thread = new ThreadedEmailMessage(email);
                        allThreads[email.InternetMessageId] = thread;
                        threads.Add(thread);
                        emails.Remove(email);
                        matched++;
                    }
                }
                if (matched == 0) break;
            }
            foreach (var email in emails)
            {
                var thread = new ThreadedEmailMessage(email);
                allThreads[email.InternetMessageId] = thread;
                threads.Add(thread);
            }

            foreach (var thread in threads)
            {
                await SetThreadConversationIndex(new[] { thread });
            }

            var emailsUpdated = 0;
            foreach (var email in allThreads.Values)
            {
                if (ByteToString(email.Message.ConversationIndex) != ByteToString(email.ConversationIndex))
                {
                    Console.WriteLine($"  {email.Message.DateTimeSent} - {ByteToString(email.Message.ConversationIndex)} -> {ByteToString(email.ConversationIndex)}");
                    if (!dryRun)
                    {
                        email.Message.SetExtendedProperty(pidTagConversationIndex, email.ConversationIndex.ToArray());
                        await Retry("save email", () => email.Message.Update(ConflictResolutionMode.AutoResolve, true));
                    }
                    emailsUpdated++;
                }
            }
            return emailsUpdated;
        }

        static async System.Threading.Tasks.Task SetThreadConversationIndex(IList<ThreadedEmailMessage> emailChain)
        {
            // There are two separate specifications for the 5 bytes of time data in the conversation index header:
            //   Specification for Outlook: https://docs.microsoft.com/en-us/office/client-developer/outlook/mapi/tracking-conversations
            //   Specification for Exchange: https://docs.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxomsg/9e994fbb-b839-495f-84e3-2c8c02c7dd9b
            //
            // We'll use the Exchange specification, which avoids Outlook's year 2057 problem. This occurs because
            // bits above 56 are not known, which limits the value to 456.69 years from 1601, i.e. year 2057.69. As a
            // sort key, this is not strictly necessary, however, avoiding the wrapping during 2057 doesn't loose us
            // much, as precision only drops to 1.678 seconds (from 0.00655 seconds).
            //
            // Timestamp in opening index:
            //   If Outlook, assume bit 56 = 1 and store bits 55 - 16 (up to 456.69 years at 0.00655 seconds precision)
            //   If Exchange, store bits 63 - 24 (up to 58455.80 years at 1.678 seconds precision)
            // Timestamp delta in subsequent indexes is either:
            //   If <= 48 bits, store bits 48 - 18 (up to 1.78 years at 0.026 seconds precision)
            //   If >= 49 bits, store bits 53 - 23 (up to 57.08 years at 0.839 seconds precision)

            var conversationStartTime = new DateTimeOffset(emailChain.First().Message.DateTimeSent);
            var email = emailChain[emailChain.Count - 1];
            var parentEmail = emailChain.Count > 1 ? emailChain[emailChain.Count - 2] : null;

            if (emailChain.Count() == 1)
            {
                // Root email in chain
                var conversationIndex = new List<byte>();
                conversationIndex.Add(1);
                conversationIndex.AddRange(BigEndianLongToBytes(conversationStartTime.ToFileTime()).Take(5));
                conversationIndex.AddRange(email.Message.ConversationIndex.Skip(6).Take(16));
                email.ConversationIndex = conversationIndex.ToArray();
            }
            else
            {
                // Non-root email in chain
                var conversationIndex = new List<byte>(parentEmail.ConversationIndex);
                var timeDiff100NS = (long)(email.Message.DateTimeSent - conversationStartTime).TotalMilliseconds * 10000;
                if ((timeDiff100NS & 0x00FE000000000000) == 0)
                {
                    conversationIndex.AddRange(BigEndianLongToBytes((timeDiff100NS >> 18) & 0x7FFFFFFF).Skip(4));
                }
                else
                {
                    conversationIndex.AddRange(BigEndianLongToBytes(0x10000000 | ((timeDiff100NS >> 23) & 0x7FFFFFFF)).Skip(4));
                }
                conversationIndex.Add(0);
                email.ConversationIndex = conversationIndex.ToArray();
            }

            foreach (var reply in emailChain.Last().Children)
            {
                await SetThreadConversationIndex(emailChain.Append(reply).ToList());
            }
        }

        static async Task<T> Retry<T>(string name, Func<Task<T>> action)
        {
            while (true)
            {
                try
                {
                    return await action();
                }
                catch (ServerBusyException error)
                {
                    Console.WriteLine($"Retry of {name} due to server busy (back off for {error.BackOffMilliseconds} ms)");
                    Thread.Sleep(error.BackOffMilliseconds);
                }
            }
        }

        static string ByteToString(IEnumerable<byte> bytes)
        {
            return String.Join("", bytes.Select(b => b.ToString("X2")));
        }

        static IEnumerable<byte> BigEndianLongToBytes(long number)
        {
            for (var bit = 56; bit >= 0; bit -= 8)
            {
                yield return (byte)(number >> bit);
            }
        }

        [DebuggerDisplay("{Message.DateTimeSent} - {MutableChildren.Count}")]
        class ThreadedEmailMessage
        {
            public EmailMessage Message;
            public ImmutableList<ThreadedEmailMessage> Children { get => MutableChildren.Values.ToImmutableList(); }
            public byte[] ConversationIndex { get; set; }

            SortedList<string, ThreadedEmailMessage> MutableChildren;

            public ThreadedEmailMessage(EmailMessage message)
            {
                Message = message;
                MutableChildren = new(StringComparer.Ordinal);
            }

            public void AddChild(ThreadedEmailMessage thread)
            {
                MutableChildren.Add($"{thread.Message.DateTimeSent.ToUniversalTime():u} {thread.Message.InternetMessageId} {thread.Message.Id}", thread);
            }
        }
    }
}

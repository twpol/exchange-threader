using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Diagnostics;
using System.IO;
using System.Linq;
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
            var conversations = await service.FindConversation(new ConversationIndexedItemView(int.MaxValue), folder.Id);
            var conversationTopics = conversations.Select(c => c.Topic).Distinct();

            var emailsUpdated = 0;
            foreach (var conversationTopic in conversationTopics)
            {
                emailsUpdated += await FixEmailConversationTopic(service, conversationTopic, dryRun);
            }

            if (dryRun)
            {
                Console.WriteLine($"  Would have updated {emailsUpdated} emails");
            }
            else
            {
                Console.WriteLine($"  Updated {emailsUpdated} emails");
            }
        }

        static async System.Threading.Tasks.Task<Folder> GetFolder(ExchangeService service, string path)
        {
            return await GetChildFolder(await Folder.Bind(service, WellKnownFolderName.MsgFolderRoot), path.Split("/").ToArray());
        }

        static async System.Threading.Tasks.Task<Folder> GetChildFolder(Folder folder, string[] children)
        {
            var folders = await folder.FindFolders(new SearchFilter.IsEqualTo(FolderSchema.DisplayName, children[0]), new FolderView(10));
            if (folders.Folders.Count != 1) throw new InvalidDataException($"Cannot find folder level '{children[0]}' below '{folder.DisplayName}'");
            if (children.Length > 1)
            {
                return await GetChildFolder(folders.Folders[0], children.Skip(1).ToArray());
            }
            return folders.Folders[0];
        }

        static async System.Threading.Tasks.Task<int> FixEmailConversationTopic(ExchangeService service, string conversationTopic, bool dryRun)
        {
            if (String.IsNullOrWhiteSpace(conversationTopic)) return 0;
            Console.WriteLine(conversationTopic);

            // Find Outlook's own search folder "AllItems", which includes all folders in the account.
            var allItems = await service.FindFolders(WellKnownFolderName.Root, new SearchFilter.IsEqualTo(FolderSchema.DisplayName, "AllItems"), new FolderView(10));
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
                all = await allItems.Folders[0].FindItems(emailFilter, emailView);
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
                foreach (var email in emails.ToArray())
                {
                    if (!messageIds.Contains(email.InReplyTo))
                    {
                        var thread = new ThreadedEmailMessage(email);
                        allThreads[email.InternetMessageId] = thread;
                        threads.Add(thread);
                        emails.Remove(email);
                        matched++;
                    }
                    else if (allThreads.ContainsKey(email.InReplyTo))
                    {
                        var thread = new ThreadedEmailMessage(email);
                        allThreads[email.InternetMessageId] = thread;
                        allThreads[email.InReplyTo].AddChild(thread);
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
                        await email.Message.Update(ConflictResolutionMode.AutoResolve, true);
                    }
                    emailsUpdated++;
                }
            }
            return emailsUpdated;
        }

        static async System.Threading.Tasks.Task SetThreadConversationIndex(IList<ThreadedEmailMessage> emailChain)
        {
            // From https://social.msdn.microsoft.com/Forums/office/en-US/4a5b4890-0f37-4b10-b3e2-495182581d34/msoxomsg-pidtagconversationindex-description-incorrect?forum=os_exchangeprotocols:
            //      Right now I know that there are at least two algorithms in use for computing the FILETIME field:
            //          Outlook client: Reserves the first byte, then uses bits 55-16 of the current FILETIME value to complete the field.
            //          Exchange: Reserves the first byte, then uses bits 63-32 of the current FILETIME value to complete the field.
            // Specification for Outlook client: https://docs.microsoft.com/en-us/office/client-developer/outlook/mapi/tracking-conversations
            // Specification for Exchange: https://docs.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxomsg/9e994fbb-b839-495f-84e3-2c8c02c7dd9b

            // We'll use the Exchange specification, which avoids the Outlook client's year 2057 problem (which is
            // when the high byte of FILETIME rolls over from 0x01 to 0x02), whilst still giving 1.6777216 second
            // accuracy in the conversation index (which does not need more precision as it is only a sort key).

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
        class ThreadedEmailMessage : IComparer<DateTime>
        {
            public EmailMessage Message;
            public ImmutableList<ThreadedEmailMessage> Children { get => MutableChildren.Values.ToImmutableList(); }
            public byte[] ConversationIndex { get; set; }

            SortedList<DateTime, ThreadedEmailMessage> MutableChildren;

            public ThreadedEmailMessage(EmailMessage message)
            {
                Message = message;
                MutableChildren = new(this);
            }

            public void AddChild(ThreadedEmailMessage thread)
            {
                MutableChildren.Add(thread.Message.DateTimeSent, thread);
            }

            public int Compare(DateTime x, DateTime y)
            {
                return (int)(x - y).TotalMilliseconds;
            }
        }
    }
}

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

        const int ConversationIndexRootEpsilonS = 10;

        static bool DryRun;
        static bool Verbose;
        static bool Debug;

        /// <summary>
        /// A command-line tool for fixing threading issues on Exchange
        /// </summary>
        /// <param name="folder">Exchange folder to scan for conversations to fix</param>
        /// <param name="config">Path to configuration file</param>
        /// <param name="conversation">Only process a single conversation topic</param>
        /// <param name="verbose">Display more details about what's going on</param>
        /// <param name="debug">Display even more details about what's going on</param>
        /// <param name="dryRun">Do not perform any actions, only pretend</param>
        static void Main(string folder, FileInfo config = null, string conversation = null, bool verbose = false, bool debug = false, bool dryRun = false)
        {
            if (config == null) config = new FileInfo("config.json");
            if (String.IsNullOrEmpty(folder)) throw new InvalidOperationException("Must specify Exchange folder to scan");
            Verbose = verbose;
            Debug = debug;
            DryRun = dryRun;
            if (DryRun) Console.WriteLine("DRY RUN mode - no changes will be saved");
            RunAsync(LoadConfiguration(config), folder, conversation).Wait();
        }

        static IConfigurationRoot LoadConfiguration(FileInfo config)
        {
            return new ConfigurationBuilder()
                .AddJsonFile(config.FullName, true)
                .Build();
        }

        static async System.Threading.Tasks.Task RunAsync(IConfigurationRoot config, string folderPath, string conversation)
        {
            var service = new ExchangeService(ExchangeVersion.Exchange2016);
            service.Credentials = new WebCredentials(config["username"], config["password"]);
            service.AutodiscoverUrl(config["email"], redirectionUri => new Uri(redirectionUri).Scheme == "https");

            var emailsUpdated = 0;
            if (conversation == null)
            {
                if (Verbose) Console.WriteLine($"VERBOSE: Looking for folder '{folderPath}'...");
                var folder = await GetFolder(service, folderPath);

                if (Verbose) Console.WriteLine($"VERBOSE: Looking for conversations...");
                var conversations = await Retry("find conversations", () => service.FindConversation(new ConversationIndexedItemView(int.MaxValue), folder.Id));
                var conversationTopics = conversations.Select(c => c.Topic).Distinct();

                foreach (var conversationTopic in conversationTopics)
                {
                    emailsUpdated += await FixEmailConversationTopic(service, conversationTopic);
                }
            }
            else
            {
                emailsUpdated += await FixEmailConversationTopic(service, conversation);
            }

            if (DryRun) Console.WriteLine($"Would have updated {emailsUpdated} emails");
            if (!DryRun) Console.WriteLine($"Updated {emailsUpdated} emails");
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

        static async System.Threading.Tasks.Task<int> FixEmailConversationTopic(ExchangeService service, string conversationTopic)
        {
            if (String.IsNullOrWhiteSpace(conversationTopic)) return 0;

            if (Verbose) Console.WriteLine($"VERBOSE: Scanning conversation '{conversationTopic}'...");

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

            if (Debug)
            {
                Console.WriteLine($"DEBUG: Emails in conversation '{conversationTopic}'...");
                foreach (var email in emails.OrderBy(email => ByteToString(email.ConversationIndex)))
                {
                    Console.WriteLine($"DEBUG:   {GetEmailDebug(email)}");
                }
            }

            var threads = new List<ThreadedEmailMessage>();
            var threadsByIndex = new Dictionary<string, ThreadedEmailMessage>();
            var threadsById = new Dictionary<string, ThreadedEmailMessage>();
            var messageIds = new HashSet<string>(emails.Select(email => email.InternetMessageId));
            var messageIndexes = new HashSet<string>(emails.Select(email => ByteToString(email.ConversationIndex)));

            while (emails.Count > 0)
            {
                var matched = 0;
                foreach (var email in emails.OrderBy(email => email.DateTimeSent).ToArray())
                {
                    var self = ByteToString(email.ConversationIndex);
                    var parent = ByteToString(email.ConversationIndex.Take(email.ConversationIndex.Length - 5));
                    var parentIds = email.References?.Split(' ') ?? email.InReplyTo?.Split(' ') ?? new string[0];
                    if (messageIndexes.Contains(parent))
                    {
                        if (threadsByIndex.ContainsKey(parent))
                        {
                            // Child of an existing message
                            var thread = new ThreadedEmailMessage(email);
                            threadsByIndex[self] = thread;
                            threadsById[email.InternetMessageId] = thread;
                            threadsByIndex[parent].AddChild(thread);
                            emails.Remove(email);
                            matched++;
                        }
                    }
                    else if (parentIds.Any(id => messageIds.Contains(id)))
                    {
                        var parentId = parentIds.Where(id => messageIds.Contains(id)).First();
                        if (threadsById.ContainsKey(parentId))
                        {
                            // Child of an existing message
                            var thread = new ThreadedEmailMessage(email);
                            threadsByIndex[self] = thread;
                            threadsById[email.InternetMessageId] = thread;
                            threadsById[parentId].AddChild(thread);
                            emails.Remove(email);
                            matched++;
                        }
                    }
                    else
                    {
                        // Root message
                        var thread = new ThreadedEmailMessage(email);
                        threadsByIndex[self] = thread;
                        threadsById[email.InternetMessageId] = thread;
                        foreach (var id in parentIds)
                        {
                            if (!messageIds.Contains(id))
                            {
                                Console.WriteLine($"WARNING: Email parent {id} was not found: {conversationTopic} - {GetEmailDebug(email)}");
                                threadsById[id] = thread;
                                messageIds.Add(id);
                            }
                        }
                        threads.Add(thread);
                        emails.Remove(email);
                        matched++;
                    }
                }
                if (matched == 0) break;
            }
            foreach (var email in emails)
            {
                // Orphaned message
                var thread = new ThreadedEmailMessage(email);
                threadsByIndex[ByteToString(email.ConversationIndex)] = thread;
                threadsById[email.InternetMessageId] = thread;
                threads.Add(thread);
            }

            if (Debug)
            {
                Console.WriteLine($"DEBUG: Threads in conversation '{conversationTopic}'...");
                foreach (var thread in threads)
                {
                    DebugThreadedEmail(thread, 0);
                }
            }

            foreach (var thread in threads)
            {
                await SetThreadConversationIndex(conversationTopic, new[] { thread });
            }

            if (Debug) Console.WriteLine($"DEBUG: Email updates in conversation '{conversationTopic}'...");
            var emailsUpdated = 0;
            var minReceived = DateTime.MaxValue;
            var maxReceived = DateTime.MinValue;
            foreach (var email in threadsByIndex.Values)
            {
                var oldIndex = ByteToString(email.Message.ConversationIndex);
                var newIndex = ByteToString(email.ConversationIndex);
                if (oldIndex != newIndex)
                {
                    if (Debug)
                    {
                        Console.WriteLine($"DEBUG:   {email.Message.DateTimeSent} - {oldIndex}");
                        Console.WriteLine($"DEBUG:                     --> {newIndex}");
                        Console.WriteLine($"DEBUG:                         {GetStringDiff(oldIndex, newIndex)}");
                    }
                    else if (Verbose)
                    {
                        Console.WriteLine($"VERBOSE:   {email.Message.DateTimeSent} - {oldIndex} --> {newIndex}");
                    }
                    if (email.Message.DateTimeReceived < minReceived) minReceived = email.Message.DateTimeReceived;
                    if (email.Message.DateTimeReceived > maxReceived) maxReceived = email.Message.DateTimeReceived;
                    if (!DryRun)
                    {
                        email.Message.SetExtendedProperty(pidTagConversationIndex, email.ConversationIndex.ToArray());
                        await Retry("save email", () => email.Message.Update(ConflictResolutionMode.AutoResolve, true));
                    }
                    emailsUpdated++;
                }
            }
            if (emailsUpdated > 0)
            {
                if (DryRun) Console.WriteLine($"Would have updated {emailsUpdated} emails received between {minReceived:u} and {maxReceived:u} in topic {conversationTopic}");
                if (!DryRun) Console.WriteLine($"Updated {emailsUpdated} emails received between {minReceived:u} and {maxReceived:u} in topic {conversationTopic}");
            }
            return emailsUpdated;
        }

        static void DebugThreadedEmail(ThreadedEmailMessage thread, int depth)
        {
            Console.WriteLine($"DEBUG:   {new String(' ', depth * 2)}{GetEmailDebug(thread.Message)}");
            foreach (var child in thread.Children)
            {
                DebugThreadedEmail(child, depth + 1);
            }
        }

        static string GetEmailDebug(EmailMessage email)
        {
            var inReplyToCount = email.InReplyTo?.Split(' ').Length ?? 0;
            var referenceCount = email.References?.Split(' ').Length ?? 0;
            return $"{email.DateTimeSent} --> {email.DateTimeReceived} - {inReplyToCount,2}/{referenceCount,-2} - {ByteToString(email.ConversationIndex)}";
        }

        static async System.Threading.Tasks.Task SetThreadConversationIndex(string conversationTopic, IList<ThreadedEmailMessage> emailChain)
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

            var email = emailChain[emailChain.Count - 1];
            var parentEmail = emailChain.Count > 1 ? emailChain[emailChain.Count - 2] : null;

            if (parentEmail == null)
            {
                // Root email in chain
                if (Debug)
                {
                    var startTimeSent = new DateTimeOffset(email.Message.DateTimeSent);
                    var startTimeIndex = email.Message.ConversationIndex.Skip(1).First() < 128
                        ? DateTimeOffset.FromFileTime(BytesToBigEndianLong(email.Message.ConversationIndex.Skip(1).Take(5).Concat(new byte[] { 0, 0, 0 })))
                        : DateTimeOffset.FromFileTime(BytesToBigEndianLong(email.Message.ConversationIndex.Skip(1).Take(5).Prepend((byte)1).Concat(new byte[] { 0, 0 })));
                    var startTimeDiff = startTimeSent - startTimeIndex;
                    if (Math.Abs(startTimeDiff.TotalSeconds) > ConversationIndexRootEpsilonS)
                    {
                        Console.WriteLine($"DEBUG: Email conversation index has start time {startTimeIndex}; expected {startTimeSent} (sent time); difference {startTimeDiff}: {conversationTopic} - {GetEmailDebug(email.Message)}");
                    }
                }

                email.ConversationIndex = email.Message.ConversationIndex.Take(22).ToArray();
            }
            else if (email.Message.ConversationIndex.Length >= 27)
            {
                // Non-root email in chain (with response level block)
                email.ConversationIndex = parentEmail.ConversationIndex.Concat(email.Message.ConversationIndex.TakeLast(5)).ToArray();
            }
            else
            {
                // Non-root email in chain (without response level block)
                var conversationIndex = new List<byte>(parentEmail.ConversationIndex);
                var timeDiff100NS = (long)(email.Message.DateTimeSent - emailChain.First().Message.DateTimeSent).TotalMilliseconds * 10000;
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
                await SetThreadConversationIndex(conversationTopic, emailChain.Append(reply).ToList());
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
            return String.Join("", bytes.Select((b, i) => b.ToString("X2") + GetByteStringSeparator(i)));
        }

        static string GetByteStringSeparator(int index)
        {
            if (index == 0 || index == 5) return " ";
            if (index == 9 || index == 11 || index == 13 || index == 15) return "-";
            if (index >= 21 && index % 5 == 1) return " ";
            return "";
        }

        static string GetStringDiff(string a, string b)
        {
            return String.Join("", Enumerable.Range(0, Math.Max(a.Length, b.Length)).Select(i => i >= a.Length || i >= b.Length ? '-' : a[i] != b[i] ? '~' : ' '));
        }

        static IEnumerable<byte> BigEndianLongToBytes(long number)
        {
            for (var bit = 56; bit >= 0; bit -= 8)
            {
                yield return (byte)(number >> bit);
            }
        }

        static long BytesToBigEndianLong(IEnumerable<byte> bytes)
        {
            return bytes.Reverse().Select((b, i) => (long)b << (i * 8)).Sum();
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

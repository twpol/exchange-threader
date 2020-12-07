using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Exchange.WebServices.Data;
using Microsoft.Extensions.Configuration;

namespace Exchange_Threader
{
    class Program
    {
        /// <param name="folder">Exchange folder to scan for conversations to fix</param>
        /// <param name="config">Path to configuration file</param>
        /// <param name="dryRun">Do not perform any actions, only pretend</param>
        static void Main(string folder, FileInfo config = null, bool dryRun = false)
        {
            if (config == null) config = new FileInfo("config.json");
            if (String.IsNullOrEmpty(folder)) throw new InvalidOperationException("Must specify Exchange folder to scan");
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

            Console.WriteLine($"Looking for folder '{folderPath}...");
            var folder = await GetFolder(service, folderPath);

            Console.WriteLine($"Looking for conversations...");
            var conversations = await service.FindConversation(new ConversationIndexedItemView(int.MaxValue), folder.Id);
            var conversationTopics = conversations.Select(c => c.Topic).Distinct();

            foreach (var conversationTopic in conversationTopics)
            {
                await FixEmailConversationTopic(service, conversationTopic, dryRun);
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

        static async System.Threading.Tasks.Task FixEmailConversationTopic(ExchangeService service, string conversationTopic, bool dryRun)
        {
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
                PropertySet = new PropertySet(BasePropertySet.IdOnly, ItemSchema.DateTimeSent, ItemSchema.DateTimeReceived, EmailMessageSchema.ConversationIndex, EmailMessageSchema.InternetMessageId, EmailMessageSchema.InReplyTo),
            };

            List<EmailMessage> emails = new();
            FindItemsResults<Item> all;
            do
            {
                all = await allItems.Folders[0].FindItems(emailFilter, emailView);
                foreach (var item in all.Items)
                {
                    emails.Add(item as EmailMessage);
                }
                emailView.Offset = all.NextPageOffset ?? 0;
            } while (all.MoreAvailable);

            var groupedEmails = GroupEmailsByMessageId(emails).SelectMany(GroupEmailsByDate);
            foreach (var group in groupedEmails)
            {
                await SetConversationIndex(group, dryRun);
            }
        }

        static List<List<EmailMessage>> GroupEmailsByMessageId(List<EmailMessage> emails)
        {
            var threads = new Dictionary<string, List<EmailMessage>>();
            var lastEmailCount = emails.Count;
            while (emails.Count > 0)
            {
                foreach (var email in emails.ToArray())
                {
                    if (email.InReplyTo == null)
                    {
                        threads[email.InternetMessageId] = new();
                        threads[email.InternetMessageId].Add(email);
                        emails.Remove(email);
                    }
                    else if (threads.ContainsKey(email.InReplyTo))
                    {
                        threads[email.InternetMessageId] = threads[email.InReplyTo];
                        threads[email.InReplyTo].Add(email);
                        emails.Remove(email);
                    }
                }
                if (lastEmailCount == emails.Count) break;
                lastEmailCount = emails.Count;
            }
            foreach (var email in emails)
            {
                threads[email.InternetMessageId] = new();
                threads[email.InternetMessageId].Add(email);
            }
            return threads.Values.Select(e => e).Distinct().ToList();
        }

        static List<List<EmailMessage>> GroupEmailsByDate(List<EmailMessage> emails)
        {
            var emailGroups = new List<List<EmailMessage>>();
            emailGroups.Add(new());
            foreach (var email in emails.OrderBy(email => email.DateTimeSent))
            {
                if (emailGroups.Last().Count > 0 && (email.DateTimeSent - emailGroups.Last().Last().DateTimeSent).TotalDays > 90)
                {
                    emailGroups.Add(new());
                }
                emailGroups.Last().Add(email);
            }
            return emailGroups;
        }

        static async System.Threading.Tasks.Task SetConversationIndex(IEnumerable<EmailMessage> emails, bool dryRun)
        {
            // From https://social.msdn.microsoft.com/Forums/office/en-US/4a5b4890-0f37-4b10-b3e2-495182581d34/msoxomsg-pidtagconversationindex-description-incorrect?forum=os_exchangeprotocols:
            //      Right now I know that there are at least two algorithms in use for computing the FILETIME field:
            //          Outlook client: Reserves the first byte, then uses bits 55-16 of the current FILETIME value to complete the field. This is what is documented in [MS-OXOMSG] §2.2.1.3.
            //          Exchange: Reserves the first byte, then uses bits 63-32 of the current FILETIME value to complete the field. This is not currently documented.
            // Specification for Outlook client: https://docs.microsoft.com/en-us/office/client-developer/outlook/mapi/tracking-conversations
            // Specification for Exchange: https://docs.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxomsg/9e994fbb-b839-495f-84e3-2c8c02c7dd9b

            // We'll use the Exchange specification, which avoids the Outlook client's year 2057 problem (which is
            // when the high byte of FILETIME rolls over from 0x01 to 0x02), whilst still giving 1.6777216 second
            // accuracy in the conversation index (which does not need more precision as it is only a sort key).

            // Specification: https://docs.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxprops/57f8de0f-5f53-423a-8947-7943dd959997
            var pidTagConversationIndex = new ExtendedPropertyDefinition(0x0071, MapiPropertyType.Binary);

            // Specification: https://docs.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxprops/76c027d8-c871-4e40-9a14-28f6596a7732
            var pidTagMessageDeliveryTime = new ExtendedPropertyDefinition(0x0E06, MapiPropertyType.SystemTime);

            var conversationStartTime = DateTimeOffset.MinValue;
            var conversationIndex = new List<byte>();

            foreach (var email in emails.OrderBy(email => email.DateTimeSent))
            {
                if (conversationIndex.Count == 0)
                {
                    conversationStartTime = new DateTimeOffset(email.DateTimeSent);
                    conversationIndex.Add(1);
                    conversationIndex.AddRange(BigEndianLongToBytes(conversationStartTime.ToFileTime()).Take(5));
                    conversationIndex.AddRange(email.ConversationIndex.Skip(6).Take(16));
                }
                else
                {
                    var timeDiff100NS = (long)(email.DateTimeSent - conversationStartTime).TotalMilliseconds * 10000;
                    if ((timeDiff100NS & 0x00FE000000000000) == 0)
                    {
                        conversationIndex.AddRange(BigEndianLongToBytes((timeDiff100NS >> 18) & 0x7FFFFFFF).Skip(4));
                    }
                    else
                    {
                        conversationIndex.AddRange(BigEndianLongToBytes(0x10000000 | ((timeDiff100NS >> 23) & 0x7FFFFFFF)).Skip(4));
                    }
                    conversationIndex.Add(0);
                }
                Console.WriteLine($"  {email.DateTimeSent} -> {email.DateTimeReceived} - {ByteToString(email.ConversationIndex)} -> {ByteToString(conversationIndex)}");
                if (!dryRun)
                {
                    email.SetExtendedProperty(pidTagMessageDeliveryTime, email.DateTimeSent);
                    email.SetExtendedProperty(pidTagConversationIndex, conversationIndex.ToArray());
                    await email.Update(ConflictResolutionMode.AutoResolve, true);
                }
            }
            if (!dryRun) Console.WriteLine($"  Updated {emails.Count()} emails");
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
    }
}

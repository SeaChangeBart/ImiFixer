using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reactive.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using ImiFixer.Properties;

namespace ImiFixer
{
    class ScheduleEvent
    {
        public string Crid { get; set; }
        public string ServiceId { get; set; }
        public string Imi { get; set; }
        public DateTime StartTime { get; set; }
    }

    static class Extensions
    {
        public static IObservable<string> ToObservableSimple(this FileSystemWatcher src)
        {
            var sources = new[] 
            { 
                Observable.FromEvent<FileSystemEventHandler, FileSystemEventArgs>( handler => (sender, e) => handler(e), eh => src.Deleted += eh,  eh => src.Deleted -= eh ),
                Observable.FromEvent<FileSystemEventHandler, FileSystemEventArgs>( handler => (sender, e) => handler(e), eh => src.Created += eh,  eh => src.Created -= eh ),
                Observable.FromEvent<FileSystemEventHandler, FileSystemEventArgs>( handler => (sender, e) => handler(e), eh => src.Changed += eh,  eh => src.Changed -= eh ),
            };
            return sources.Merge().Select(ev => ev.FullPath);
        }

        public static IObservable<string> ObserveFileSystemIncludingExisting(this string srcDir, string filter)
        {
            return srcDir.ObserveFileSystem(filter).Merge(Directory.GetFiles(srcDir, filter).ToObservable());
        }

        public static IObservable<string> ObserveFileSystem(this string srcDir, string filter)
        {
            return Observable.Create<string>(
                subscriber =>
                {
                    var fsm = new FileSystemWatcher
                    {
                        Filter = filter,
                        Path = srcDir,
                        IncludeSubdirectories = true
                    };
                    fsm.ToObservableSimple().Subscribe(subscriber);
                    fsm.EnableRaisingEvents = true;
                    return fsm;
                });
        }

        public static IObservable<T> ThrottlePerValue<T>(this IObservable<T> src, TimeSpan throttleValue)
        {
            return src.GroupBy(_ => _).SelectMany(gr => gr.Throttle(throttleValue));
        }
    }

    class Program
    {
        private static Dictionary<string, ScheduleEvent[]> _eventsByCrid;
        private static Dictionary<string, ScheduleEvent[]> _eventsByService;
        private static string FindImi(string crid, string service, DateTime startTime)
        {
            ScheduleEvent[] list;
            if (!_eventsByCrid.TryGetValue(crid, out list))
                return null;

            return
                list.Where(se => se.ServiceId.Equals(service) && se.StartTime.Equals(startTime))
                    .Select(se => se.Imi)
                    .FirstOrDefault();
        }

        static readonly XNamespace tvaNs = "urn:tva:metadata:2010";
        static readonly XNamespace tvaNs2008 = "urn:tva:mpeg7:2008";

        private static void ReadExistingImis(string source)
        {
            Console.WriteLine("Loading TVAs sent to Prodis");
            var files = Directory.EnumerateFiles(source, "*_TVA.xml");
            var docs = files.Select(XDocument.Load);
            var tvaEvents =
                docs.SelectMany(doc=> doc.Root.Element(tvaNs + "ProgramDescription").Element(tvaNs + "ProgramLocationTable")
                    .Descendants(tvaNs + "ScheduleEvent")
                    .Select(ScheduleEventFromSEElement)).OrderBy( e=>e.StartTime).ToArray();
            Console.WriteLine("{0} events", tvaEvents.Length);
            _eventsByCrid = tvaEvents.GroupBy(e => e.Crid).ToDictionary(g => g.Key, g => g.ToArray());
            _eventsByService = tvaEvents.GroupBy(e => e.ServiceId).ToDictionary(g => g.Key, g => g.ToArray());
            Console.WriteLine("Optimized");
        }

        private static ScheduleEvent ScheduleEventFromSEElement(XElement sEl)
        {
            return new ScheduleEvent
            {
                Crid = sEl.Elements(tvaNs + "Program").Single().Attributes("crid").Single().Value,
                Imi = sEl.Elements(tvaNs + "InstanceMetadataId").Single().Value,
                ServiceId = sEl.Parent.Attributes("serviceIDRef").Single().Value,
                StartTime =
                    DateTime.Parse(sEl.Elements(tvaNs + "PublishedStartTime").Single().Value, null,
                        DateTimeStyles.RoundtripKind),
            };
        }

        private static ScheduleEvent ScheduleEventFromBEElement(XElement sEl)
        {
            return new ScheduleEvent
            {
                Crid = sEl.Elements(tvaNs + "Program").Single().Attributes("crid").Single().Value,
                Imi =
                    sEl.Elements(tvaNs+"InstanceDescription").Single().Elements(tvaNs + "RelatedMaterial")
                        .Single(
                            rmel =>
                                rmel.Elements(tvaNs + "HowRelated")
                                    .Any(
                                        hrEl =>
                                            hrEl.Attribute("href")
                                                .Value.Equals("urn:tva:metadata:cs:HowRelatedCS:2010:16")))
                        .Elements(tvaNs + "MediaLocator").Single()
                        .Elements(tvaNs2008+"MediaUri").Single().Value,
                ServiceId = sEl.Attributes("serviceIDRef").Single().Value,
                StartTime =
                    DateTime.Parse(sEl.Elements(tvaNs + "PublishedStartTime").Single().Value, null,
                        DateTimeStyles.RoundtripKind),
            };
        }

        private static void Main()
        {
            var srcDir = Settings.Default.WatchFolder;
            var tgtDir = Settings.Default.OutputFolder;
            var refPath = Settings.Default.RefFolder;
            ReadExistingImis(refPath);

            using (
                srcDir.ObserveFileSystemIncludingExisting("*.xml")
                    .ThrottlePerValue(TimeSpan.FromSeconds(1))
                    .Where(File.Exists)
                    .Subscribe(fn => TryFixFile(fn, tgtDir)))
            {
                Console.WriteLine("Esc to exit");
                while (Console.ReadKey().Key != ConsoleKey.Escape)
                    Console.WriteLine("Esc to exit");
            }
        }

        private static void TryFixFile(string src, string destDir)
        {
            int n = 0;
            while (n++ < 10)
            {
                try
                {
                    FixFile(src, destDir);
                    return;
                }
                catch
                {
                    Thread.Sleep(10);
                }
            }
            Console.WriteLine("Failed to process {0}", src);
        }

        static void FixFile(string src, string destDir)
        {
            var dest = Path.Combine(destDir, Path.GetFileName(src));
            var doc = XDocument.Load(src);
            var ses = 0;
            var fix = 0;
            var found = 0;
            var serviceId = "";
            foreach (var seEl in doc.Descendants(tvaNs + "ScheduleEvent"))
            {
                var se = ScheduleEventFromSEElement(seEl);
                serviceId = se.ServiceId;
                var oldImi = FindImi(se.Crid, se.ServiceId, se.StartTime);
                ses++;
                if (oldImi == null)
                    continue;
                found++;
                if (oldImi.Equals(se.Imi))
                    continue;
                seEl.Elements(tvaNs + "InstanceMetadataId").Single().Value = oldImi;
                fix++;
            }
            var eventsForService = NumEventsForService(serviceId);
            Console.WriteLine("Fixed {0} imis, Same {1} imis in {2} events in {3} [{4} BEs for that service]", fix, found - fix, ses, Path.GetFileName(src), eventsForService);
            doc.Save(dest);
            File.Delete(src);
        }

        private static int NumEventsForService(string serviceId)
        {
            if (_eventsByService.ContainsKey(serviceId))
                return _eventsByService[serviceId].Length;
            return 0;
        }
    }
}

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace FotoServer
{
    class CacheItem
    {
        public byte[] Data { get; set; } = null!;
        public DateTime Timestamp { get; set; }
        public string ContentType { get; set; } = null!;
    }

    class ImageServer
    {
        private readonly HttpListener _listener;
        private readonly string _rootPath;
        private readonly int _maxConcurrentRequests;

        private readonly Dictionary<string, CacheItem> _cache = new();
        private readonly object _cacheLock = new object();
        private readonly HashSet<string> _pendingRequests = new();

        private readonly TimeSpan _cacheDuration = TimeSpan.FromMinutes(5);

        private static readonly object _logLock = new object();
        private readonly SemaphoreSlim _semaphore;

        
        private readonly BlockingCollection<HttpListenerContext> _requestQueue = new();
        private readonly CancellationTokenSource _cts = new();

       
        private Thread? _cacheCleanupThread;

        private static void Log(string message)
        {
            lock (_logLock)
            {
                Console.WriteLine($"[{DateTime.Now:HH:mm:ss.fff}] [Nit {Thread.CurrentThread.ManagedThreadId}] {message}");
            }
        }

        public ImageServer(string rootPath, int port, int maxRequests)
        {
            _rootPath = rootPath;
            _maxConcurrentRequests = maxRequests;
            _semaphore = new SemaphoreSlim(maxRequests, maxRequests);
            _listener = new HttpListener();
            _listener.Prefixes.Add($"http://localhost:{port}/");
        }

        public void Start()
        {
            _listener.Start();
            Log($"Server pokrenut na portu 5050. Root: {_rootPath}");
            Log($"Maksimalan broj paralelnih obrada: {_maxConcurrentRequests}");

            
            _cacheCleanupThread = new Thread(CleanupCacheLoop) { IsBackground = true };
            _cacheCleanupThread.Start();

            
            Task.Run(() => ConsumeRequests(), _cts.Token);

            try
            {
                while (_listener.IsListening && !_cts.Token.IsCancellationRequested)
                {
                   
                    HttpListenerContext context = _listener.GetContext();
                    Log($"Primljen zahtev: {context.Request.Url?.AbsolutePath} -> Smešta se u red.");
                    _requestQueue.Add(context);
                }
            }
            catch (HttpListenerException) when (!_listener.IsListening)
            {
                Log("Listener je uspešno zaustavljen.");
            }
            catch (Exception ex)
            {
                Log($"[GREŠKA] Neočekivana greška u petlji: {ex.Message}");
            }
        }

        public void Stop()
        {
            Log("Pokrenuta procedura za zaustavljanje servera...");
            _cts.Cancel(); 
            _requestQueue.CompleteAdding(); 
            if (_listener.IsListening)
            {
                _listener.Stop();
            }
        }

        
        private void CleanupCacheLoop()
        {
            while (!_cts.Token.IsCancellationRequested)
            {
                
                bool cancelled = _cts.Token.WaitHandle.WaitOne(TimeSpan.FromMinutes(1));
                if (cancelled) break;

                lock (_cacheLock)
                {
                    var keysToRemove = new List<string>();
                    foreach (var kvp in _cache)
                    {
                        if (DateTime.Now - kvp.Value.Timestamp >= _cacheDuration)
                        {
                            keysToRemove.Add(kvp.Key);
                        }
                    }

                    foreach (var key in keysToRemove)
                    {
                        _cache.Remove(key);
                        Log($"[Keš Čistač] Keš istekao za '{key}', stavka je obrisana.");
                    }
                }
            }
            Log("[Keš Čistač] Nit za čišćenje keša zaustavljena.");
        }


        private void ConsumeRequests()
        {
            try
            {
                foreach (var context in _requestQueue.GetConsumingEnumerable(_cts.Token))
                {
                    _semaphore.Wait(_cts.Token); 

                   
                    Task processTask = Task.Run(async () => await ProcessRequestAsync(context), _cts.Token);

       
                    processTask.ContinueWith(t =>
                    {
                        _semaphore.Release();
                        if (t.IsFaulted)
                        {
                            Log($"[GREŠKA] Task je bacio izuzetak: {t.Exception?.GetBaseException().Message}");
                        }
                    }, TaskContinuationOptions.ExecuteSynchronously);
                }
            }
            catch (OperationCanceledException)
            {
                Log("Obrada zahteva iz reda je prekinuta (Cancel).");
            }
        }

        private async Task ProcessRequestAsync(HttpListenerContext context)
        {
            try
            {
                string fileName = context.Request.Url!.AbsolutePath.TrimStart('/');
                Log($"Obrada zahteva za: {fileName}");

                if (string.IsNullOrEmpty(fileName) || !IsImageFile(fileName))
                {
                    Log($"Odbijen zahtev — nije slika ili prazan naziv: '{fileName}'");
                    await SendErrorAsync(context, "Samo slike su dozvoljene (.png, .jpg, .jpeg, .gif).", 400);
                    return;
                }

                var (imageData, contentType) = await GetImageDataAsync(fileName);

                if (imageData != null)
                {
                    Log($"Slanje slike '{fileName}' ({imageData.Length} bajtova, {contentType})");
                    context.Response.ContentType = contentType;
                    context.Response.ContentLength64 = imageData.Length;
                    await context.Response.OutputStream.WriteAsync(imageData, 0, imageData.Length);
                }
                else
                {
                    Log($"Slika nije pronađena: '{fileName}'");
                    await SendErrorAsync(context, $"Slika '{fileName}' nije pronađena.", 404);
                }
            }
            catch (Exception ex)
            {
                Log($"[GREŠKA] Greška pri obradi zahteva: {ex.Message}");
                try { await SendErrorAsync(context, "Interna greška servera.", 500); } catch { }
            }
            finally
            {
                try { context.Response.Close(); } catch { }
            }
        }

        private async Task<(byte[]? Data, string ContentType)> GetImageDataAsync(string fileName)
        {
            string contentType = "image/png";

            lock (_cacheLock)
            {
           
                if (_cache.TryGetValue(fileName, out var item))
                {
                    if (DateTime.Now - item.Timestamp < _cacheDuration)
                    {
                        Log("--- CACHE HIT ---");
                        return (item.Data, item.ContentType);
                    }
                }

              
                while (_pendingRequests.Contains(fileName))
                {
                    Log("Task čeka na rezultat druge obrade (Stampede prevention)...");
                    Monitor.Wait(_cacheLock);

                    if (_cache.TryGetValue(fileName, out item) && DateTime.Now - item.Timestamp < _cacheDuration)
                    {
                        Log("--- CACHE HIT (nakon čekanja) ---");
                        return (item.Data, item.ContentType);
                    }
                }

    
                _pendingRequests.Add(fileName);
            }

            byte[]? data = null;
            try
            {
       
                data = await FindAndReadFileAsync(fileName);

                if (data != null)
                {
                    contentType = GetContentType(fileName);
                    lock (_cacheLock)
                    {
                        _cache[fileName] = new CacheItem
                        {
                            Data = data,
                            Timestamp = DateTime.Now,
                            ContentType = contentType
                        };
                        Log($"Fajl '{fileName}' dodat u keš ({data.Length} bajtova).");
                    }
                }
            }
            catch (Exception ex)
            {
                Log($"[GREŠKA] Greška pri čitanju fajla '{fileName}': {ex.Message}");
            }
            finally
            {
                lock (_cacheLock)
                {
                    _pendingRequests.Remove(fileName);
                    Monitor.PulseAll(_cacheLock); 
                }
            }

            return (data, contentType);
        }

        private async Task<byte[]?> FindAndReadFileAsync(string fileName)
        {
            var files = Directory.GetFiles(_rootPath, fileName, SearchOption.AllDirectories);
            if (files.Length > 0)
            {
                Log($"Pronađen fajl: {files[0]}");
                return await File.ReadAllBytesAsync(files[0]);
            }
            return null;
        }

        private bool IsImageFile(string fileName)
        {
            string ext = Path.GetExtension(fileName).ToLower();
            return ext == ".png" || ext == ".jpg" || ext == ".jpeg" || ext == ".gif";
        }

        private string GetContentType(string fileName)
        {
            string ext = Path.GetExtension(fileName).ToLower();
            return ext switch
            {
                ".png" => "image/png",
                ".jpg" or ".jpeg" => "image/jpeg",
                ".gif" => "image/gif",
                _ => "application/octet-stream"
            };
        }

        private async Task SendErrorAsync(HttpListenerContext context, string message, int code)
        {
            try
            {
                context.Response.StatusCode = code;
                byte[] buffer = System.Text.Encoding.UTF8.GetBytes(message);
                context.Response.ContentType = "text/plain; charset=utf-8";
                context.Response.ContentLength64 = buffer.Length;
                await context.Response.OutputStream.WriteAsync(buffer, 0, buffer.Length);
            }
            catch (Exception ex)
            {
                Log($"[GREŠKA] Greška pri slanju error odgovora: {ex.Message}");
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            string root = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "images");
            if (!Directory.Exists(root))
            {
                Directory.CreateDirectory(root);
                Console.WriteLine($"Kreiran folder za slike: {root}");
            }

            ImageServer server = new ImageServer(root, 5050, 10);

            
            Task serverTask = Task.Run(() => server.Start());

            Console.WriteLine("Server je pokrenut. Unesite 'stop' za bezbedno gašenje programa.");

            
            while (true)
            {
                string? input = Console.ReadLine();
                if (input?.Trim().ToLower() == "stop")
                {
                    break;
                }
            }

            server.Stop();

            
            serverTask.Wait();

            Console.WriteLine("Server je uspešno ugašen. Pritisnite bilo koji taster za izlaz...");
            Console.ReadKey();
        }
    }
}
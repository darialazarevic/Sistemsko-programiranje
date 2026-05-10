using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;

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

            
            try
            {
                while (_listener.IsListening)
                {
                    
                    HttpListenerContext context = _listener.GetContext();
                    Log($"Primljen zahtev: {context.Request.Url?.AbsolutePath}");
                    ThreadPool.QueueUserWorkItem(state => ProcessRequest(context));
                }
            }
            catch (HttpListenerException ex) when (!_listener.IsListening)
            {
                Log($"Listener zaustavljen: {ex.Message}");
            }
            catch (Exception ex)
            {
                Log($"[GREŠKA] Neočekivana greška u petlji: {ex.Message}");
            }
        }

        public void Stop()
        {
            Log("Zaustavljanje servera...");
            _listener.Stop();
        }

        private void ProcessRequest(HttpListenerContext context)
        {
            
            _semaphore.Wait();
            try
            {
                string fileName = context.Request.Url!.AbsolutePath.TrimStart('/');
                Log($"Obrada zahteva za: {fileName}");

                if (string.IsNullOrEmpty(fileName) || !IsImageFile(fileName))
                {
                    Log($"Odbijen zahtev — nije slika ili prazan naziv: '{fileName}'");
                    SendError(context, "Samo slike su dozvoljene (.png, .jpg, .jpeg, .gif).", 400);
                    return;
                }

                byte[]? imageData = GetImageData(fileName, out string contentType);

                if (imageData != null)
                {
                    Log($"Slanje slike '{fileName}' ({imageData.Length} bajtova, {contentType})");
                    context.Response.ContentType = contentType;
                    context.Response.ContentLength64 = imageData.Length;
                    context.Response.OutputStream.Write(imageData, 0, imageData.Length);
                }
                else
                {
                    Log($"Slika nije pronađena: '{fileName}'");
                    SendError(context, $"Slika '{fileName}' nije pronađena.", 404);
                }
            }
            catch (Exception ex)
            {
                
                Log($"[GREŠKA] Greška pri obradi zahteva: {ex.Message}");
                try { SendError(context, "Interna greška servera.", 500); } catch { }
            }
            finally
            {
                try { context.Response.Close(); } catch { }
                _semaphore.Release();
            }
        }

        private byte[]? GetImageData(string fileName, out string contentType)
        {
            contentType = "image/png";

            lock (_cacheLock)
            {
                
                if (_cache.TryGetValue(fileName, out var item))
                {
                    if (DateTime.Now - item.Timestamp < _cacheDuration)
                    {
                        Log("--- CACHE HIT ---");
                        contentType = item.ContentType;
                        return item.Data;
                    }
                    _cache.Remove(fileName);
                    Log($"Keš istekao za '{fileName}', briše se.");
                }

                
                while (_pendingRequests.Contains(fileName))
                {
                    Log("Nit čeka na rezultat druge niti (Stampede prevention)...");
                    Monitor.Wait(_cacheLock); 

                    
                    if (_cache.TryGetValue(fileName, out item) &&
                        DateTime.Now - item.Timestamp < _cacheDuration)
                    {
                        Log("--- CACHE HIT (nakon čekanja) ---");
                        contentType = item.ContentType;
                        return item.Data;
                    }
                }

                
                _pendingRequests.Add(fileName);
            }

            
            byte[]? data = null;
            try
            {
                data = FindAndReadFile(fileName);

                lock (_cacheLock)
                {
                    if (data != null)
                    {
                        
                        contentType = GetContentType(fileName);
                        _cache[fileName] = new CacheItem
                        {
                            Data = data,
                            Timestamp = DateTime.Now,
                            ContentType = contentType
                        };
                        Log($"Fajl '{fileName}' dodat u keš ({data.Length} bajtova).");
                    }
                }

                return data;
            }
            catch (Exception ex)
            {
                
                Log($"[GREŠKA] Greška pri čitanju fajla '{fileName}': {ex.Message}");
                return null;
            }
            finally
            {
                lock (_cacheLock)
                {
                    _pendingRequests.Remove(fileName);
                    Monitor.PulseAll(_cacheLock); 
                }
            }
        }

        private byte[]? FindAndReadFile(string fileName)
        {
            
            var files = Directory.GetFiles(_rootPath, fileName, SearchOption.AllDirectories);
            if (files.Length > 0)
            {
                Log($"Pronađen fajl: {files[0]}");
                return File.ReadAllBytes(files[0]);
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

        private void SendError(HttpListenerContext context, string message, int code)
        {
            try
            {
                context.Response.StatusCode = code;
                byte[] buffer = System.Text.Encoding.UTF8.GetBytes(message);
                context.Response.ContentType = "text/plain; charset=utf-8";
                context.Response.ContentLength64 = buffer.Length;
                context.Response.OutputStream.Write(buffer, 0, buffer.Length);
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

            

            server.Start();
        }
    }
}
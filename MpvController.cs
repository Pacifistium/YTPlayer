using System;
using System.Diagnostics;
using System.IO;
using System.IO.Pipes;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace YTPlayer
{
    public class MpvController : IDisposable
    {
        private Process? _process;
        private NamedPipeClientStream? _pipe;
        private StreamWriter? _writer;
        private readonly string _mpvPath;
        private readonly string _ytdlpPath;
        private string _pipeName = "ytpipe";
        private bool _connected;
        private bool _stopping;

        // Громкость, которую нужно применить сразу после IPC-подключения
        private int _pendingVolume = -1;

        public event Action<string>? TitleChanged;
        public event Action<double>? DurationChanged;
        public event Action<double>? PositionChanged;
        public event Action? PlaybackEnded;
        public event Action<string>? ErrorOutput;

        public MpvController(string basePath)
        {
            // mpv может быть mpv.exe или mpv.com (.com = консольная сборка, поддерживает IPC)
            var mpvExe = Path.Combine(basePath, "mpv.exe");
            var mpvCom = Path.Combine(basePath, "mpv.com");
            var mpvRaw = File.Exists(mpvCom) ? mpvCom : mpvExe;

            _mpvPath   = GetShortPath(mpvRaw);
            _ytdlpPath = GetShortPath(Path.Combine(basePath, "yt-dlp.exe"));
        }

        public bool MpvExists   => File.Exists(_mpvPath) ||
                                   File.Exists(Path.ChangeExtension(_mpvPath, ".exe")) ||
                                   File.Exists(Path.ChangeExtension(_mpvPath, ".com"));
        public bool YtdlpExists => File.Exists(_ytdlpPath);

        // ─── Короткий 8.3 путь (обходим лимит 260 символов) ──────────────
        private static string GetShortPath(string longPath)
        {
            if (!File.Exists(longPath)) return longPath; // вернём как есть, ошибку поймаем позже
            var sb = new System.Text.StringBuilder(512);
            uint len = NativeMethods.GetShortPathName(longPath, sb, (uint)sb.Capacity);
            return len > 0 ? sb.ToString() : longPath;
        }

        public async Task PlayAsync(string url, int volume = 80)
        {
            await StopAsync();

            // Короткое имя пайпа: максимум ~16 символов, без пробелов и спецсимволов
            _pipeName = "ytp" + Guid.NewGuid().ToString("N")[..8];
            _stopping = false;
            _pendingVolume = volume;

            _process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName        = _mpvPath,
                    UseShellExecute = false,
                    CreateNoWindow  = true,
                    RedirectStandardError = true,
                },
                EnableRaisingEvents = true
            };

            var args = _process.StartInfo.ArgumentList;
            args.Add("--no-video");
            args.Add("--no-terminal");
            args.Add("--msg-level=all=warn");
            // Передаём yt-dlp через короткий путь — избегаем длинных аргументов
            args.Add($"--script-opts=ytdl_hook-ytdl_path={_ytdlpPath}");
            args.Add($"--volume={volume}");
            args.Add($"--input-ipc-server={_pipeName}");
            args.Add(url);

            ErrorOutput?.Invoke($"mpv args: --volume={volume} --input-ipc-server={_pipeName} [url]");
            ErrorOutput?.Invoke($"ytdlp path: {_ytdlpPath}");

            _process.Exited += (s, e) =>
            {
                ErrorOutput?.Invoke($"mpv exited: {_process?.ExitCode}");
                if (!_stopping) PlaybackEnded?.Invoke();
            };
            _process.ErrorDataReceived += (s, e) =>
            {
                if (e.Data != null) ErrorOutput?.Invoke($"[mpv] {e.Data}");
            };

            _process.Start();
            _process.BeginErrorReadLine();

            _ = Task.Run(async () =>
            {
                await Task.Delay(600);
                if (_process != null && _process.HasExited)
                    ErrorOutput?.Invoke($"mpv died early! ExitCode={_process.ExitCode}");
                else
                    ErrorOutput?.Invoke("mpv running OK");
            });

            _ = Task.Run(ConnectPipeAsync);
        }

        private async Task ConnectPipeAsync()
        {
            // mpv открывает пайп чуть позже старта — ждём
            await Task.Delay(1200);

            for (int i = 0; i < 20; i++)
            {
                if (_stopping) return;
                try
                {
                    _pipe   = new NamedPipeClientStream(".", _pipeName, PipeDirection.InOut, PipeOptions.Asynchronous);
                    await _pipe.ConnectAsync(1000);
                    _writer = new StreamWriter(_pipe, Encoding.UTF8) { AutoFlush = true };
                    _connected = true;

                    // Подписываемся на свойства
                    await SendCommandAsync("observe_property", 1, "media-title");
                    await SendCommandAsync("observe_property", 2, "duration");
                    await SendCommandAsync("observe_property", 3, "time-pos");

                    // FIX 1: Принудительно запрашиваем title — на случай если
                    // observe_property пришёл позже чем mpv загрузил трек
                    await SendCommandAsync("get_property", "media-title");

                    // FIX 3: Применяем отложенную громкость
                    if (_pendingVolume >= 0)
                    {
                        await SendCommandAsync("set_property", "volume", _pendingVolume);
                        _pendingVolume = -1;
                    }

                    _ = Task.Run(ReadLoopAsync);
                    ErrorOutput?.Invoke("IPC connected OK");
                    return;
                }
                catch
                {
                    _pipe?.Dispose();
                    _pipe = null;
                    await Task.Delay(300);
                }
            }
            ErrorOutput?.Invoke("IPC: не удалось подключиться после 20 попыток");
        }

        private async Task ReadLoopAsync()
        {
            if (_pipe == null) return;
            var reader = new StreamReader(_pipe, Encoding.UTF8);
            try
            {
                while (_connected && _pipe.IsConnected)
                {
                    var line = await reader.ReadLineAsync();
                    if (line == null) break;
                    ParseEvent(line);
                }
            }
            catch { }
        }

        private void ParseEvent(string json)
        {
            try
            {
                using var doc = JsonDocument.Parse(json);
                var root = doc.RootElement;

                // Ответ на get_property: {"error":"success","data":"Title","request_id":0}
                if (root.TryGetProperty("error", out var err) && err.GetString() == "success" &&
                    root.TryGetProperty("data", out var directData) &&
                    directData.ValueKind == JsonValueKind.String)
                {
                    var val = directData.GetString();
                    if (!string.IsNullOrEmpty(val))
                        TitleChanged?.Invoke(val);
                    return;
                }

                // Ответ на observe_property: {"event":"property-change","name":"...","data":...}
                if (root.TryGetProperty("event", out var ev) && ev.GetString() == "property-change")
                {
                    if (!root.TryGetProperty("name", out var nameProp)) return;
                    var name = nameProp.GetString();
                    if (!root.TryGetProperty("data", out var data)) return;

                    switch (name)
                    {
                        case "media-title":
                            if (data.ValueKind == JsonValueKind.String)
                            {
                                var title = data.GetString();
                                if (!string.IsNullOrEmpty(title))
                                    TitleChanged?.Invoke(title);
                            }
                            break;
                        case "duration":
                            if (data.ValueKind == JsonValueKind.Number)
                                DurationChanged?.Invoke(data.GetDouble());
                            break;
                        case "time-pos":
                            if (data.ValueKind == JsonValueKind.Number)
                                PositionChanged?.Invoke(data.GetDouble());
                            break;
                    }
                }
            }
            catch { }
        }

        public Task PauseAsync()                  => SendCommandAsync("cycle", "pause");
        public Task SeekAsync(double seconds)     => SendCommandAsync("seek", seconds, "relative");
        public Task SeekAbsoluteAsync(double sec) => SendCommandAsync("seek", sec, "absolute");

        // FIX 3: Если IPC ещё не готов — запоминаем как _pendingVolume
        public Task SetVolumeAsync(int vol)
        {
            if (!_connected)
            {
                _pendingVolume = vol;
                return Task.CompletedTask;
            }
            return SendCommandAsync("set_property", "volume", vol);
        }

        private async Task SendCommandAsync(params object[] args)
        {
            if (!_connected || _writer == null) return;
            try
            {
                var cmd  = new { command = args };
                var json = JsonSerializer.Serialize(cmd);
                await _writer.WriteLineAsync(json);
            }
            catch { _connected = false; }
        }

        public async Task StopAsync()
        {
            _stopping      = true;
            _connected     = false;
            _pendingVolume = -1;

            try { _writer?.Close(); }   catch { }
            try { _pipe?.Close(); _pipe?.Dispose(); } catch { }
            _writer = null;
            _pipe   = null;

            var proc = _process;
            _process = null;

            if (proc != null)
            {
                try
                {
                    if (!proc.HasExited)
                    {
                        ErrorOutput?.Invoke($"Killing mpv PID={proc.Id}...");
                        proc.Kill(entireProcessTree: true);
                        var exited = await Task.Run(() => proc.WaitForExit(3000));
                        ErrorOutput?.Invoke(exited ? "mpv killed OK" : "mpv kill timeout");
                    }
                    else ErrorOutput?.Invoke("mpv already exited");
                }
                catch (Exception ex) { ErrorOutput?.Invoke($"Kill failed: {ex.Message}"); }
                finally { proc.Dispose(); }
            }
        }

        public bool IsRunning => _process != null && !_process.HasExited;

        public void Dispose() => _ = StopAsync();
    }

    internal static class NativeMethods
    {
        [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Auto)]
        internal static extern uint GetShortPathName(
            string lpszLongPath,
            System.Text.StringBuilder lpszShortPath,
            uint cchBuffer);
    }
}

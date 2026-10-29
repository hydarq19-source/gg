use std::{
    collections::HashMap,
    env,
    error::Error,
    io,
    io::{ErrorKind, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use log::{error, info};
use tokio::fs;
use whatsapp_rust::{
    media::{DocumentOptions, document_message},
    pair_code::PairCodeOptions,
    prelude::*,
    wacore::download::MediaType,
};
use yt_dlp::{
    Downloader, VideoSelection,
    client::deps::Libraries,
    download::{ManagerConfig, SpeedProfile},
    model::selector::{AudioCodecPreference, AudioQuality, VideoCodecPreference, VideoQuality},
};

const WORK_DIR: &str = "downloads";
const REQUIRED_YTDLP_VERSION: &str = "2026.10.26";
const DEFAULT_CPU_EMA_TARGET_PERCENT: u32 = 55;
const DEFAULT_CPU_EMA_ALPHA: f64 = 0.25;
const DEFAULT_UPLOAD_MEMORY_MAX_MB: u64 = 512;
const MAX_UPLOAD_MEMORY_MAX_MB: u64 = 4096;
static PENDING_YOUTUBE_URLS: OnceLock<Mutex<HashMap<String, String>>> = OnceLock::new();
static YOUTUBE_DOWNLOAD_SLOT: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(1);

fn pending_youtube_urls() -> &'static Mutex<HashMap<String, String>> {
    PENDING_YOUTUBE_URLS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    if Path::new(".env").exists() {
        dotenvy::from_path(".env")?;
    }

    let phone_raw = env::var("WHATSAPP_PHONE_NUMBER").map_err(|_| {
        io::Error::new(
            ErrorKind::InvalidInput,
            "WHATSAPP_PHONE_NUMBER is missing; set it in .env.",
        )
    })?;
    let phone_number = normalize_phone(&phone_raw)?;
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn")).init();
    fs::create_dir_all(WORK_DIR).await?;
    cleanup_stale_downloads().await;

    let cpu_ema = Arc::new(AtomicU32::new(0));
    let cpu_ema_sampling_paused = Arc::new(AtomicBool::new(false));
    let cpu_ema_pause_epoch = Arc::new(AtomicU64::new(0));
    let cpu_ema_alpha = env::var("CPU_EMA_ALPHA")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|alpha| alpha.is_finite())
        .unwrap_or(DEFAULT_CPU_EMA_ALPHA)
        .clamp(0.05, 0.8);
    let cpu_ema_target_percent = env::var("CPU_EMA_TARGET_PERCENT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(DEFAULT_CPU_EMA_TARGET_PERCENT)
        .clamp(1, 100);
    tokio::spawn(sample_cgroup_cpu_ema(
        Arc::clone(&cpu_ema),
        Arc::clone(&cpu_ema_sampling_paused),
        Arc::clone(&cpu_ema_pause_epoch),
        cpu_ema_alpha,
    ));
    info!(
        "Adaptive CPU guard enabled (EMA alpha={cpu_ema_alpha:.2}, target={cpu_ema_target_percent}%)."
    );

    let store = SqliteStore::new("whatsapp_bot.db").await?;
    let bot = Bot::builder()
        .with_backend(store)
        .with_pair_code(PairCodeOptions {
            phone_number,
            ..Default::default()
        })
        .on_pair_code(|code, timeout| async move {
            println!(
                "\nPairing code (valid for {} seconds):\n\n  {}\n",
                timeout.as_secs(),
                code
            );
            println!("On your phone: WhatsApp > Linked devices > Link a device > Link with phone number instead.");
        })
        .on_pair_code_error(|err, _client| async move {
            if let Some(rejection) = err.rejection {
                error!("Pairing-code request rejected: {rejection:?}");
            } else {
                error!("Pairing-code request failed locally or timed out.");
            }
            if let Some(backoff) = err.backoff {
                error!("Server retry delay: {} seconds.", backoff.as_secs());
            }
        })
        .on_connected(|_client| async {
            info!("WhatsApp connected; the bot is ready.");
        })
        .on_logged_out(|_info| async {
            error!("WhatsApp device was logged out. Pair it again.");
        })
        .on_message({
            let cpu_ema = Arc::clone(&cpu_ema);
            let cpu_ema_sampling_paused = Arc::clone(&cpu_ema_sampling_paused);
            let cpu_ema_pause_epoch = Arc::clone(&cpu_ema_pause_epoch);
            move |ctx| {
                let cpu_ema = Arc::clone(&cpu_ema);
                let cpu_ema_sampling_paused = Arc::clone(&cpu_ema_sampling_paused);
                let cpu_ema_pause_epoch = Arc::clone(&cpu_ema_pause_epoch);
                async move {
                    let Some(text) = ctx.message.text_content() else {
                        return;
                    };
                    let command = text.trim();

                    if command == "/yt" || command == "/yt help" {
                        let _ = ctx.reply(yt_help()).await;
                        return;
                    }
                    if command.starts_with("/yt ") {
                        let ctx_for_job = ctx.clone();
                        let args = command[4..].trim().to_owned();
                        let cpu_ema = Arc::clone(&cpu_ema);
                        tokio::spawn(async move {
                            if let Err(err) = handle_yt_command(
                                &ctx_for_job,
                                &args,
                                cpu_ema,
                                cpu_ema_sampling_paused,
                                cpu_ema_pause_epoch,
                                cpu_ema_target_percent,
                            )
                            .await
                            {
                                error!("YouTube command failed: {err}");
                                let _ = ctx_for_job.reply(format!("تعذر تنفيذ الطلب: {err}")).await;
                            }
                        });
                        return;
                    }

                    if let Some(response) = response_for_text(command)
                        && let Err(err) = ctx.reply(response).await
                    {
                        error!("Failed to send a reply: {err}");
                    }
                }
            }
        })
        .build()
        .await?;

    info!("Starting WhatsApp bot. Session data is stored in whatsapp_bot.db.");
    let mut handle = bot.spawn();
    tokio::select! {
        reason = &mut handle => info!("Bot stopped: {reason:?}"),
        signal = tokio::signal::ctrl_c() => {
            signal?;
            info!("Shutdown signal received; closing gracefully.");
            let report = handle.shutdown().await;
            info!("Shutdown complete: {report:?}");
        }
    }
    Ok(())
}

async fn handle_yt_command(
    ctx: &MessageContext,
    args: &str,
    cpu_ema: Arc<AtomicU32>,
    cpu_ema_sampling_paused: Arc<AtomicBool>,
    cpu_ema_pause_epoch: Arc<AtomicU64>,
    cpu_ema_target_percent: u32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let (video_quality, audio_quality, supplied_url) = match parse_yt_args(args) {
        Ok(parsed) => parsed,
        Err(message) => {
            ctx.reply(message).await?;
            return Ok(());
        }
    };
    let _download_permit = match YOUTUBE_DOWNLOAD_SLOT.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            ctx.reply(
                "هناك طلب تنزيل آخر قيد التنفيذ لتخفيف حمل المعالج. أعد المحاولة بعد اكتماله.",
            )
            .await?;
            return Ok(());
        }
    };
    let chat_key = ctx.info.source.chat.to_string();
    let url = if let Some(url) = supplied_url {
        pending_youtube_urls()
            .lock()
            .expect("pending URL lock poisoned")
            .insert(chat_key.clone(), url.clone());
        url
    } else {
        let Some(url) = pending_youtube_urls()
            .lock()
            .expect("pending URL lock poisoned")
            .get(&chat_key)
            .cloned()
        else {
            ctx.reply("أرسل /yt <الرابط> أولًا لعرض الجودات، ثم أرسل /yt <رقم الفيديو> <رقم الصوت>.")
                .await?;
            return Ok(());
        };
        url
    };

    // Exclude the initial URL-inspection/list reply from the CPU EMA so its
    // metadata-fetch work cannot influence adaptive delays during later uploads.
    let _ema_sampling_pause = video_quality.is_none().then(|| {
        CpuEmaSamplingPause::new(
            Arc::clone(&cpu_ema_sampling_paused),
            Arc::clone(&cpu_ema_pause_epoch),
        )
    });

    let downloader = build_downloader().await?;
    let video = downloader.fetch_video_infos(&url).await?;

    if video_quality.is_none() {
        ctx.reply(format_quality_list(&video)).await?;
        return Ok(());
    }

    let video_index = video_quality.unwrap();
    let audio_index = audio_quality.unwrap();
    let video_options = available_video_options(&video);
    let audio_options = available_audio_options(&video);
    if video_index > video_options.len() || audio_index > audio_options.len() {
        ctx.reply("رقم جودة غير صالح. أرسل /yt مرة أخرى لرؤية القائمة.")
            .await?;
        return Ok(());
    }
    if video_index == 0 && audio_index == 0 {
        ctx.reply("اختر جودة واحدة على الأقل؛ لا يمكن أن تكون القيمتان 0.")
            .await?;
        return Ok(());
    }

    ctx.reply("بدأ التنزيل. سيتم إرسال كل ملف كمرفق ثم حذفه مباشرةً...")
        .await?;
    let stem = safe_filename(&format!("{}-{}", video.title, video.id));

    if video_index != 0 {
        let (height, _) = video_options[video_index - 1];
        let selected = video
            .select_video_format(
                VideoQuality::CustomHeight(height),
                VideoCodecPreference::Any,
            )
            .ok_or("جودة الفيديو المحددة غير متاحة")?;
        let path = PathBuf::from(WORK_DIR).join(format!(
            "{stem}-video.{}",
            selected.download_info.ext.as_str()
        ));
        let expected_path = path.clone();
        let path = downloader
            .download_video_stream_with_quality_to_path(
                &video,
                path.clone(),
                VideoQuality::CustomHeight(height),
                VideoCodecPreference::Any,
            )
            .await;
        let path = match path {
            Ok(path) => path,
            Err(err) => {
                remove_file(&expected_path).await;
                return Err(Box::new(err));
            }
        };
        let send_result = send_as_document(
            ctx,
            &path,
            &format!("{stem}-video.{}", selected.download_info.ext.as_str()),
            video.title.as_str(),
            "video/*",
            Arc::clone(&cpu_ema),
            cpu_ema_target_percent,
        )
        .await;
        remove_file(&path).await;
        send_result?;
    }

    if audio_index != 0 {
        let (bitrate, _) = audio_options[audio_index - 1];
        let selected = video
            .select_audio_format(
                AudioQuality::CustomBitrate(bitrate),
                AudioCodecPreference::Any,
            )
            .ok_or("جودة الصوت المحددة غير متاحة")?;
        let path = PathBuf::from(WORK_DIR).join(format!(
            "{stem}-audio.{}",
            selected.download_info.ext.as_str()
        ));
        let expected_path = path.clone();
        let path = downloader
            .download_audio_stream_with_quality_to_path(
                &video,
                path.clone(),
                AudioQuality::CustomBitrate(bitrate),
                AudioCodecPreference::Any,
            )
            .await;
        let path = match path {
            Ok(path) => path,
            Err(err) => {
                remove_file(&expected_path).await;
                return Err(Box::new(err));
            }
        };
        let send_result = send_as_document(
            ctx,
            &path,
            &format!("{stem}-audio.{}", selected.download_info.ext.as_str()),
            video.title.as_str(),
            "audio/*",
            Arc::clone(&cpu_ema),
            cpu_ema_target_percent,
        )
        .await;
        remove_file(&path).await;
        send_result?;
    }

    pending_youtube_urls()
        .lock()
        .expect("pending URL lock poisoned")
        .remove(&chat_key);
    ctx.reply("اكتمل الطلب وتم حذف الملفات المؤقتة.").await?;
    Ok(())
}

/// Extracts a YouTube URL from a command token, accepting all common host forms.
fn extract_youtube_url(text: &str) -> Option<String> {
    for raw in text.split_whitespace() {
        let candidate = raw.trim_matches(|c: char| "<>\"'[]{}(),;".contains(c));
        let lower = candidate.to_ascii_lowercase();
        let Some(separator) = lower.find("://") else {
            continue;
        };
        let host_start = separator + 3;
        let host_end = lower[host_start..]
            .find(['/', '?', '#'])
            .map(|offset| host_start + offset)
            .unwrap_or(lower.len());
        let host = lower[host_start..host_end].trim_end_matches(':');
        let accepted = host == "youtu.be"
            || host.ends_with(".youtu.be")
            || host == "youtube.com"
            || host.ends_with(".youtube.com")
            || host == "youtube-nocookie.com"
            || host.ends_with(".youtube-nocookie.com");
        if accepted && (lower.starts_with("http://") || lower.starts_with("https://")) {
            return Some(candidate.to_owned());
        }
    }
    None
}

fn parse_yt_args(args: &str) -> Result<(Option<usize>, Option<usize>, Option<String>), String> {
    let url = extract_youtube_url(args);
    let numbers: Vec<&str> = args
        .split_whitespace()
        .filter(|part| extract_youtube_url(part).is_none())
        .collect();

    match numbers.as_slice() {
        [] if url.is_some() => Ok((None, None, url)),
        [video, audio] => {
            let video = video
                .parse::<usize>()
                .map_err(|_| "جودة الفيديو يجب أن تكون رقمًا صحيحًا.".to_owned())?;
            let audio = audio
                .parse::<usize>()
                .map_err(|_| "جودة الصوت يجب أن تكون رقمًا صحيحًا.".to_owned())?;
            Ok((Some(video), Some(audio), url))
        }
        _ => Err("الاستخدام: /yt <الرابط> ثم /yt <جودة الفيديو> <جودة الصوت>".to_owned()),
    }
}

async fn build_downloader() -> Result<Downloader, Box<dyn Error + Send + Sync>> {
    let libraries = prepare_libraries().await?;
    let mut config = ManagerConfig::from_speed_profile(SpeedProfile::Conservative);
    config.max_concurrent_downloads = 1;
    config.parallel_segments = 1;
    config.max_buffer_size = 2 * 1024 * 1024;

    let mut builder = Downloader::builder(libraries, WORK_DIR)
        .with_download_manager_config(config);

    if let Some(runtime) = configured_js_runtime() {
        info!("Using yt-dlp JavaScript runtime: {runtime}");
        builder = builder.add_arg("--js-runtimes").add_arg(runtime);
    } else {
        log::warn!(
            "No supported JavaScript runtime found. Install Node.js, Deno, or Bun; YouTube formats may be missing."
        );
    }

    if let Ok(cookie_file) = env::var("YTDLP_COOKIES") {
        let cookie_file = PathBuf::from(cookie_file);
        if !cookie_file.is_file() {
            return Err(format!(
                "YTDLP_COOKIES points to a missing file: {}",
                cookie_file.display()
            )
            .into());
        }
        info!("Using yt-dlp Netscape cookie file for YouTube authentication.");
        builder = builder.with_cookies(cookie_file);
    } else if let Ok(browser) = env::var("YTDLP_COOKIES_FROM_BROWSER") {
        if !browser.trim().is_empty() {
            info!("Using cookies extracted from the configured browser.");
            builder = builder.with_cookies_from_browser(browser);
        }
    }

    Ok(builder.build().await?)
}

/// Validate the bundled Linux x86-64 yt-dlp release.
async fn prepare_libraries() -> Result<Libraries, Box<dyn Error + Send + Sync>> {
    if !cfg!(target_os = "linux") || !cfg!(target_arch = "x86_64") {
        return Err(
            "هذا الإصدار من المشروع مخصص لـ Linux x86-64 فقط؛ ابنِه على target x86_64-unknown-linux-gnu."
                .into(),
        );
    }

    let youtube_path = PathBuf::from("libraries/yt-dlp");
    let ffmpeg_path = env::var_os("FFMPEG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("libraries/ffmpeg"));
    let libraries = Libraries::new(youtube_path.clone(), ffmpeg_path);

    let Some(version) = executable_output(&youtube_path, "--version") else {
        return Err(format!(
            "لا يمكن تشغيل {}. يجب أن يكون الملف التنفيذي Linux x86-64 الموجود في الحزمة.",
            youtube_path.display()
        )
        .into());
    };
    if version.trim() != REQUIRED_YTDLP_VERSION {
        return Err(format!(
            "إصدار yt-dlp غير مدعوم: {}. الإصدار المطلوب هو {}.",
            version.trim(),
            REQUIRED_YTDLP_VERSION
        )
        .into());
    }
    Ok(libraries)
}

fn executable_output(path: &Path, argument: &str) -> Option<String> {
    let output = Command::new(path)
        .arg(argument)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    output.status.success().then(|| {
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    })
}

fn executable_succeeds(path: &Path, argument: &str) -> bool {
    executable_output(path, argument).is_some()
}

fn configured_js_runtime() -> Option<String> {
    if let Ok(runtime) = env::var("YTDLP_JS_RUNTIME") {
        let runtime = runtime.trim().to_owned();
        if !runtime.is_empty() {
            return Some(runtime);
        }
    }

    ["deno", "node", "bun"].iter().find_map(|runtime| {
        executable_succeeds(Path::new(runtime), "--version").then(|| (*runtime).to_owned())
    })
}

async fn send_as_document(
    ctx: &MessageContext,
    path: &Path,
    filename: &str,
    title: &str,
    mimetype_prefix: &str,
    cpu_ema: Arc<AtomicU32>,
    cpu_ema_target_percent: u32,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let encrypted_path = path.with_extension("waenc");
    let source_path = path.to_path_buf();
    let encrypted_path_for_task = encrypted_path.clone();
    let cpu_ema_for_task = Arc::clone(&cpu_ema);
    let source_len = fs::metadata(path).await?.len();
    let memory_cap = configured_upload_memory_limit_bytes();
    let memory_available = available_memory_bytes();
    let encrypt_in_memory = memory_available
        .map(|available| can_encrypt_in_memory(source_len, available, memory_cap))
        .unwrap_or(false);
    info!(
        "Encrypting media ({source_len} bytes) in {} (RAM budget: {} bytes available).",
        if encrypt_in_memory {
            "memory"
        } else {
            "temporary disk file"
        },
        memory_available.unwrap_or(0)
    );

    let encryption = tokio::task::spawn_blocking(move || -> Result<_, Box<dyn Error + Send + Sync>> {
        if encrypt_in_memory {
            let capacity = usize::try_from(source_len.saturating_add(64)).ok();
            let mut encrypted = Vec::new();
            if let Some(capacity) = capacity
                && encrypted.try_reserve_exact(capacity).is_ok()
            {
                let input = std::fs::File::open(&source_path)?;
                let reader = CpuAwareReader {
                    inner: input,
                    cpu_ema: Arc::clone(&cpu_ema_for_task),
                    target_percent: cpu_ema_target_percent,
                };
                let info = whatsapp_rust::wacore::upload::encrypt_media_streaming(
                    reader,
                    &mut encrypted,
                    MediaType::Document,
                )?;
                return Ok((info, EncryptedUploadSource::Memory(Arc::new(encrypted))));
            }
            log::warn!(
                "Could not reserve the in-memory encryption buffer; falling back to a temporary disk file."
            );
        }

        let input = std::fs::File::open(&source_path)?;
        let mut output = std::fs::File::create(&encrypted_path_for_task)?;
        let reader = CpuAwareReader {
            inner: input,
            cpu_ema: cpu_ema_for_task,
            target_percent: cpu_ema_target_percent,
        };
        let info = whatsapp_rust::wacore::upload::encrypt_media_streaming(
            reader,
            &mut output,
            MediaType::Document,
        )?;
        let encrypted_len = output.metadata()?.len();
        drop(output);
        Ok((
            info,
            EncryptedUploadSource::File(FileUploadSource {
                path: encrypted_path_for_task,
                len: encrypted_len,
            }),
        ))
    })
    .await;
    let (info, source) = match encryption {
        Ok(Ok(encrypted)) => encrypted,
        Ok(Err(err)) => {
            remove_file(&encrypted_path).await;
            return Err(err);
        }
        Err(err) => {
            remove_file(&encrypted_path).await;
            return Err(Box::new(err));
        }
    };
    let encrypted_temp_path = source.disk_path().map(Path::to_path_buf);
    let upload_result = ctx
        .client
        .upload_stream(source, info, MediaType::Document)
        .await;
    if let Some(encrypted_temp_path) = encrypted_temp_path.as_deref() {
        remove_file(encrypted_temp_path).await;
    }
    let upload = upload_result?;
    let mimetype = if filename.ends_with(".mp4") {
        "video/mp4"
    } else if filename.ends_with(".webm") {
        if mimetype_prefix.starts_with("audio") {
            "audio/webm"
        } else {
            "video/webm"
        }
    } else if filename.ends_with(".m4a") {
        "audio/mp4"
    } else {
        "application/octet-stream"
    };
    let message = document_message(
        upload,
        DocumentOptions {
            mimetype: Some(mimetype.to_owned()),
            file_name: Some(filename.to_owned()),
            title: Some(title.to_owned()),
            ..Default::default()
        },
    );
    ctx.send_message(message).await?;
    Ok(())
}

struct FileUploadSource {
    path: PathBuf,
    len: u64,
}

enum EncryptedUploadSource {
    Memory(Arc<Vec<u8>>),
    File(FileUploadSource),
}

impl EncryptedUploadSource {
    fn disk_path(&self) -> Option<&Path> {
        match self {
            Self::Memory(_) => None,
            Self::File(source) => Some(&source.path),
        }
    }
}

impl whatsapp_rust::wacore::upload::UploadSource for EncryptedUploadSource {
    fn len(&self) -> u64 {
        match self {
            Self::Memory(data) => data.len() as u64,
            Self::File(source) => source.len,
        }
    }

    fn reader_from(&self, offset: u64) -> io::Result<Box<dyn Read + Send>> {
        match self {
            Self::Memory(data) => Ok(Box::new(MemoryUploadReader {
                data: Arc::clone(data),
                position: usize::try_from(offset)
                    .unwrap_or(usize::MAX)
                    .min(data.len()),
            })),
            Self::File(source) => {
                let mut file = std::fs::File::open(&source.path)?;
                file.seek(SeekFrom::Start(offset))?;
                Ok(Box::new(file))
            }
        }
    }
}

struct MemoryUploadReader {
    data: Arc<Vec<u8>>,
    position: usize,
}

impl Read for MemoryUploadReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let remaining = &self.data[self.position..];
        let count = remaining.len().min(buffer.len());
        buffer[..count].copy_from_slice(&remaining[..count]);
        self.position += count;
        Ok(count)
    }
}

impl whatsapp_rust::wacore::upload::UploadSource for FileUploadSource {
    fn len(&self) -> u64 {
        self.len
    }

    fn reader_from(&self, offset: u64) -> io::Result<Box<dyn Read + Send>> {
        let mut file = std::fs::File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        Ok(Box::new(file))
    }
}

struct CpuAwareReader<R> {
    inner: R,
    cpu_ema: Arc<AtomicU32>,
    target_percent: u32,
}

impl<R: Read> Read for CpuAwareReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        let current_ema = self.cpu_ema.load(Ordering::Relaxed);
        let target_ema = self.target_percent.saturating_mul(100);
        let delay = cpu_ema_backoff(current_ema, target_ema);
        if !delay.is_zero() {
            thread::sleep(delay);
        }
        self.inner.read(buffer)
    }
}

fn cpu_ema_backoff(current_ema: u32, target_ema: u32) -> Duration {
    if current_ema <= target_ema {
        return Duration::ZERO;
    }
    let excess = u64::from(current_ema - target_ema);
    Duration::from_micros((500 + excess * 10).clamp(500, 10_000))
}

fn ema_next(previous: f64, sample: f64, alpha: f64) -> f64 {
    alpha * sample + (1.0 - alpha) * previous
}

struct CpuEmaSamplingPause {
    paused: Arc<AtomicBool>,
    pause_epoch: Arc<AtomicU64>,
}

impl CpuEmaSamplingPause {
    fn new(paused: Arc<AtomicBool>, pause_epoch: Arc<AtomicU64>) -> Self {
        pause_epoch.fetch_add(1, Ordering::AcqRel);
        paused.store(true, Ordering::Release);
        Self {
            paused,
            pause_epoch,
        }
    }
}

impl Drop for CpuEmaSamplingPause {
    fn drop(&mut self) {
        self.pause_epoch.fetch_add(1, Ordering::AcqRel);
        self.paused.store(false, Ordering::Release);
    }
}

fn should_skip_cpu_ema_sample(is_paused: bool, current_epoch: u64, observed_epoch: u64) -> bool {
    is_paused || current_epoch != observed_epoch
}

async fn sample_cgroup_cpu_ema(
    cpu_ema: Arc<AtomicU32>,
    sampling_paused: Arc<AtomicBool>,
    pause_epoch: Arc<AtomicU64>,
    alpha: f64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    let mut previous: Option<(u64, Instant)> = None;
    let mut ema: Option<f64> = None;
    let mut observed_pause_epoch = pause_epoch.load(Ordering::Acquire);

    loop {
        interval.tick().await;
        let current_pause_epoch = pause_epoch.load(Ordering::Acquire);
        if should_skip_cpu_ema_sample(
            sampling_paused.load(Ordering::Acquire),
            current_pause_epoch,
            observed_pause_epoch,
        ) {
            previous = None;
            observed_pause_epoch = current_pause_epoch;
            continue;
        }

        let Some((usage_us, capacity_cores)) = read_cgroup_cpu_usage().await else {
            continue;
        };
        let current_pause_epoch = pause_epoch.load(Ordering::Acquire);
        if should_skip_cpu_ema_sample(
            sampling_paused.load(Ordering::Acquire),
            current_pause_epoch,
            observed_pause_epoch,
        ) {
            previous = None;
            observed_pause_epoch = current_pause_epoch;
            continue;
        }

        let now = Instant::now();
        let Some((previous_usage, previous_at)) = previous.replace((usage_us, now)) else {
            continue;
        };
        let elapsed_us = now.duration_since(previous_at).as_micros().max(1) as f64;
        let used_us = usage_us.saturating_sub(previous_usage) as f64;
        let sample_percent = (used_us / elapsed_us / capacity_cores * 100.0).clamp(0.0, 100.0);
        let current = ema.map_or(sample_percent, |old| ema_next(old, sample_percent, alpha));
        ema = Some(current);
        cpu_ema.store((current * 100.0).round() as u32, Ordering::Relaxed);
    }
}

async fn read_cgroup_cpu_usage() -> Option<(u64, f64)> {
    let stat = fs::read_to_string("/sys/fs/cgroup/cpu.stat").await.ok()?;
    let usage_us = stat.lines().find_map(|line| {
        let (key, value) = line.split_once(' ')?;
        (key == "usage_usec")
            .then(|| value.trim().parse::<u64>().ok())
            .flatten()
    })?;
    let limits = fs::read_to_string("/sys/fs/cgroup/cpu.max").await.ok()?;
    let mut fields = limits.split_whitespace();
    let quota = fields.next()?;
    let period_us = fields.next()?.parse::<f64>().ok()?;
    let capacity_cores = if quota == "max" {
        std::thread::available_parallelism()
            .map(|count| count.get() as f64)
            .unwrap_or(1.0)
    } else {
        quota.parse::<f64>().ok()? / period_us
    };
    (capacity_cores.is_finite() && capacity_cores > 0.0).then_some((usage_us, capacity_cores))
}

fn configured_upload_memory_limit_bytes() -> u64 {
    let megabytes = env::var("UPLOAD_MEMORY_MAX_MB")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_UPLOAD_MEMORY_MAX_MB)
        .min(MAX_UPLOAD_MEMORY_MAX_MB);
    megabytes.saturating_mul(1024 * 1024)
}

fn can_encrypt_in_memory(file_len: u64, available: u64, configured_cap: u64) -> bool {
    file_len > 0 && configured_cap > 0 && file_len <= configured_cap && file_len <= available / 4
}

fn available_memory_bytes() -> Option<u64> {
    if let Some(available) = cgroup_memory_available_bytes() {
        return Some(available);
    }

    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let available_kb = meminfo.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key == "MemAvailable")
            .then(|| value.split_whitespace().next()?.parse::<u64>().ok())
            .flatten()
    })?;
    Some(available_kb.saturating_mul(1024))
}

fn cgroup_memory_available_bytes() -> Option<u64> {
    for (limit_path, usage_path) in [
        ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"),
        (
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
            "/sys/fs/cgroup/memory/memory.usage_in_bytes",
        ),
    ] {
        let Ok(limit) = std::fs::read_to_string(limit_path) else {
            continue;
        };
        let limit = limit.trim();
        if limit == "max" {
            continue;
        }
        let limit = limit.parse::<u64>().ok()?;
        // cgroup v1 uses a huge sentinel value when no memory limit is set.
        if limit >= (1_u64 << 60) {
            continue;
        }
        let Ok(usage) = std::fs::read_to_string(usage_path) else {
            continue;
        };
        let Ok(usage) = usage.trim().parse::<u64>() else {
            continue;
        };
        return Some(limit.saturating_sub(usage));
    }
    None
}

fn is_bot_temp_download(name: &str) -> bool {
    name.ends_with(".waenc") || name.contains("-video.") || name.contains("-audio.")
}

async fn cleanup_stale_downloads() {
    let Ok(mut entries) = fs::read_dir(WORK_DIR).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        let name = entry.file_name();
        if file_type.is_file() && is_bot_temp_download(&name.to_string_lossy()) {
            remove_file(&entry.path()).await;
        }
    }
}

async fn remove_file(path: &Path) {
    if let Err(err) = fs::remove_file(path).await
        && err.kind() != ErrorKind::NotFound
    {
        error!("Failed to remove {}: {err}", path.display());
    }
}

fn format_size(bytes: Option<i64>) -> String {
    let Some(bytes) = bytes.filter(|size| *size >= 0) else {
        return "الحجم غير معروف".to_owned();
    };
    let value = bytes as f64;
    if value >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.2} GB", value / (1024.0 * 1024.0 * 1024.0))
    } else {
        format!("{:.1} MB", value / (1024.0 * 1024.0))
    }
}

fn parse_quality_index(raw: &str) -> Result<usize, Box<dyn Error + Send + Sync>> {
    Ok(raw
        .parse::<usize>()
        .map_err(|_| "الجودة يجب أن تكون رقمًا صحيحًا")?)
}

fn available_video_options(video: &yt_dlp::model::Video) -> Vec<(u32, Option<i64>)> {
    let mut values: Vec<(u32, Option<i64>)> = video
        .formats
        .iter()
        .filter(|f| f.is_video())
        .filter_map(|f| {
            f.video_resolution
                .height
                .map(|height| (height, f.file_info.filesize))
        })
        .collect();
    values.sort_by(|a, b| b.0.cmp(&a.0));
    values.dedup_by_key(|item| item.0);
    values
}

fn available_audio_options(video: &yt_dlp::model::Video) -> Vec<(u32, Option<i64>)> {
    let mut values: Vec<(u32, Option<i64>)> = video
        .formats
        .iter()
        .filter(|f| f.is_audio())
        .filter_map(|f| {
            f.rates_info
                .audio_rate
                .map(|rate| (rate.0.round() as u32, f.file_info.filesize))
        })
        .collect();
    if values.is_empty() {
        values.push((128, None));
    }
    values.sort_by(|a, b| b.0.cmp(&a.0));
    values.dedup_by_key(|item| item.0);
    values
}

fn format_quality_list(video: &yt_dlp::model::Video) -> String {
    let mut out = format!(
        "العنوان: {}\n\nجودات الفيديو (استخدم الرقم):\n0 — عدم تنزيل الفيديو\n",
        video.title
    );
    for (i, (height, size)) in available_video_options(video).iter().enumerate() {
        out.push_str(&format!(
            "{} — {}p — {}\n",
            i + 1,
            height,
            format_size(*size)
        ));
    }
    out.push_str("\nجودات الصوت:\n0 — عدم تنزيل الصوت\n");
    for (i, (rate, size)) in available_audio_options(video).iter().enumerate() {
        out.push_str(&format!(
            "{} — تقريبًا {} kbps — {}\n",
            i + 1,
            rate,
            format_size(*size)
        ));
    }
    out.push_str(
        "\nبعد ظهور هذه القائمة أرسل فقط: /yt <رقم جودة الفيديو> <رقم جودة الصوت>\nمثال: /yt 3 0",
    );
    out
}

fn safe_filename(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, ' ' | '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    cleaned
        .trim()
        .chars()
        .take(80)
        .collect::<String>()
        .replace(' ', "_")
}

fn yt_help() -> &'static str {
    "أمر يوتيوب:\n/yt <الرابط> — عرض الجودات والأحجام\nبعد القائمة أرسل فقط /yt <جودة الفيديو> <جودة الصوت> مثل /yt 3 0.\nالقيمة 0 تلغي تنزيل ذلك النوع.\nكل من الفيديو والصوت يرسل كملف مرفق وليس كفيديو/رسالة صوتية."
}

fn response_for_text(text: &str) -> Option<String> {
    match text {
        "/hi" => Some("HELOO".to_owned()),
        "/مساعدة" | "/help" | "!مساعدة" | "!help" => {
            Some("أوامر البوت:\n/hi\n/ping\n/yt — تنزيل يوتيوب\n/echo <نص>".to_owned())
        }
        "/ping" | "!ping" => Some("Pong — البوت يعمل.".to_owned()),
        _ if text.starts_with("/echo ") => Some(text[6..].trim().to_owned()),
        _ => None,
    }
}

fn normalize_phone(raw: &str) -> Result<String, io::Error> {
    if raw
        .chars()
        .any(|ch| !(ch.is_ascii_digit() || matches!(ch, '+' | '-' | '(' | ')' | ' ')))
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "WHATSAPP_PHONE_NUMBER must contain a phone number only.",
        ));
    }
    let digits: String = raw.chars().filter(char::is_ascii_digit).collect();
    if digits.len() < 7 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "WHATSAPP_PHONE_NUMBER is too short.",
        ));
    }
    Ok(digits)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phone_is_normalized() {
        assert_eq!(normalize_phone("+1 (555) 123-4567").unwrap(), "15551234567");
    }
    #[test]
    fn invalid_quality_is_rejected() {
        assert!(parse_quality_index("x").is_err());
    }
    #[test]
    fn filenames_are_safe() {
        assert_eq!(safe_filename("a/b:c"), "a_b_c");
    }
    #[test]
    fn accepts_common_youtube_hosts_and_paths() {
        for url in [
            "https://www.youtube.com/watch?v=abc123",
            "https://youtu.be/abc123?t=30",
            "https://youtube.com/shorts/abc123",
            "https://m.youtube.com/live/abc123?feature=share",
            "https://music.youtube.com/watch?v=abc123",
            "https://www.youtube-nocookie.com/embed/abc123",
        ] {
            assert_eq!(extract_youtube_url(url).as_deref(), Some(url));
        }
    }
    #[test]
    fn extracts_url_when_wrapped_in_text_or_punctuation() {
        assert_eq!(
            extract_youtube_url("شاهد: <https://youtu.be/abc123?t=4>"),
            Some("https://youtu.be/abc123?t=4".to_owned())
        );
    }
    #[test]
    fn parses_url_then_quality_only_command() {
        assert_eq!(
            parse_yt_args("3 0 https://youtu.be/abc123").unwrap(),
            (Some(3), Some(0), Some("https://youtu.be/abc123".to_owned()))
        );
        assert_eq!(parse_yt_args("0 4").unwrap(), (Some(0), Some(4), None));
    }
    #[test]
    fn formats_sizes() {
        assert_eq!(format_size(Some(1024 * 1024)), "1.0 MB");
        assert_eq!(format_size(None), "الحجم غير معروف");
    }

    #[test]
    fn ema_smooths_cpu_spikes_and_tracks_sustained_load() {
        let alpha = 0.25;
        let after_spike = ema_next(20.0, 100.0, alpha);
        assert!((after_spike - 40.0).abs() < f64::EPSILON);
        let sustained = ema_next(after_spike, 100.0, alpha);
        assert!(sustained > after_spike && sustained < 100.0);
    }

    #[test]
    fn cpu_backoff_only_applies_above_the_ema_target() {
        assert_eq!(cpu_ema_backoff(5_500, 5_500), Duration::ZERO);
        assert!(cpu_ema_backoff(8_000, 5_500) > Duration::ZERO);
        assert!(cpu_ema_backoff(100_000, 5_500) <= Duration::from_millis(10));
    }

    #[test]
    fn in_memory_encryption_obeys_configured_cap_and_ram_headroom() {
        assert!(can_encrypt_in_memory(100, 1_000, 200));
        assert!(!can_encrypt_in_memory(201, 1_000, 200));
        assert!(!can_encrypt_in_memory(100, 399, 200));
        assert!(!can_encrypt_in_memory(0, 1_000, 200));
        assert!(!can_encrypt_in_memory(1, 1_000, 0));
    }

    #[test]
    fn in_memory_upload_source_can_reopen_at_offsets() {
        let source = EncryptedUploadSource::Memory(Arc::new(vec![1, 2, 3, 4]));
        assert_eq!(whatsapp_rust::wacore::upload::UploadSource::len(&source), 4);

        let mut reader =
            whatsapp_rust::wacore::upload::UploadSource::reader_from(&source, 2).unwrap();
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining).unwrap();
        assert_eq!(remaining, [3, 4]);
    }

    #[test]
    fn startup_cleanup_only_targets_bot_download_artifacts() {
        assert!(is_bot_temp_download("video-video.mp4"));
        assert!(is_bot_temp_download("video-audio.m4a"));
        assert!(is_bot_temp_download("video-video.waenc"));
        assert!(!is_bot_temp_download("whatsapp_bot.db"));
        assert!(!is_bot_temp_download("notes.txt"));
    }

    #[test]
    fn quality_listing_pause_is_excluded_even_if_it_falls_between_samples() {
        let paused = Arc::new(AtomicBool::new(false));
        let pause_epoch = Arc::new(AtomicU64::new(0));
        let observed_epoch = pause_epoch.load(Ordering::Acquire);

        {
            let _pause = CpuEmaSamplingPause::new(Arc::clone(&paused), Arc::clone(&pause_epoch));
            assert!(paused.load(Ordering::Acquire));
            assert!(should_skip_cpu_ema_sample(
                paused.load(Ordering::Acquire),
                pause_epoch.load(Ordering::Acquire),
                observed_epoch,
            ));
        }

        assert!(!paused.load(Ordering::Acquire));
        let epoch_after_resume = pause_epoch.load(Ordering::Acquire);
        assert_eq!(epoch_after_resume, observed_epoch + 2);
        assert!(should_skip_cpu_ema_sample(
            paused.load(Ordering::Acquire),
            epoch_after_resume,
            observed_epoch,
        ));
        assert!(!should_skip_cpu_ema_sample(
            paused.load(Ordering::Acquire),
            epoch_after_resume,
            epoch_after_resume,
        ));
    }
}

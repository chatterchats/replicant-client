//! Native lifecycle shell for the Replicant web application and local daemon.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::{
    env, fs,
    io::{Read, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};

use serde::{Deserialize, Serialize};
use tauri::{
    App, AppHandle, Manager, Window, WindowEvent,
    image::Image,
    menu::{CheckMenuItemBuilder, MenuBuilder, MenuItemBuilder},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};
use tauri_plugin_shell::{
    ShellExt,
    process::{CommandChild, CommandEvent},
};

const DAEMON_BIND: &str = "127.0.0.1:8080";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
struct DesktopSettings {
    close_to_tray: bool,
}

impl Default for DesktopSettings {
    fn default() -> Self {
        Self {
            close_to_tray: true,
        }
    }
}

struct DesktopState {
    settings: Mutex<DesktopSettings>,
    settings_path: PathBuf,
    managed_daemon: Mutex<Option<CommandChild>>,
    data_dir: PathBuf,
    token_file: PathBuf,
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .setup(setup)
        .on_window_event(handle_window_event)
        .run(tauri::generate_context!())
        .expect("failed to run Replicant desktop application");
}

fn setup(app: &mut App) -> Result<(), Box<dyn std::error::Error>> {
    let config_dir = app.path().app_config_dir()?;
    let data_dir = app.path().app_local_data_dir()?;
    fs::create_dir_all(&config_dir)?;
    fs::create_dir_all(&data_dir)?;

    let settings_path = config_dir.join("desktop.json");
    let settings = load_settings(&settings_path);
    app.manage(DesktopState {
        settings: Mutex::new(settings.clone()),
        settings_path,
        managed_daemon: Mutex::new(None),
        data_dir,
        token_file: config_dir.join("api-token"),
    });

    build_tray(app, settings.close_to_tray)?;
    if let Err(error) = ensure_daemon(app.handle()) {
        eprintln!("replicantd could not be started: {error}");
    }
    Ok(())
}

fn load_settings(path: &Path) -> DesktopSettings {
    fs::read(path)
        .ok()
        .and_then(|contents| serde_json::from_slice(&contents).ok())
        .unwrap_or_default()
}

fn save_settings(state: &DesktopState) -> Result<(), Box<dyn std::error::Error>> {
    let settings = state.settings.lock().expect("settings lock poisoned");
    fs::write(&state.settings_path, serde_json::to_vec_pretty(&*settings)?)?;
    Ok(())
}

fn build_tray(app: &App, close_to_tray: bool) -> Result<(), Box<dyn std::error::Error>> {
    let open = MenuItemBuilder::with_id("open", "Open Replicant").build(app)?;
    let start = MenuItemBuilder::with_id("start-daemon", "Start local daemon").build(app)?;
    let close = CheckMenuItemBuilder::with_id("close-to-tray", "Close window to tray")
        .checked(close_to_tray)
        .build(app)?;
    let quit = MenuItemBuilder::with_id("quit", "Quit (leave automation running)").build(app)?;
    let quit_all =
        MenuItemBuilder::with_id("quit-all", "Quit and stop managed automation").build(app)?;
    let menu = MenuBuilder::new(app)
        .items(&[&open, &start, &close, &quit, &quit_all])
        .build()?;
    let close_item = close.clone();

    TrayIconBuilder::with_id("main")
        .icon(tray_image())
        .menu(&menu)
        .show_menu_on_left_click(false)
        .on_menu_event(move |app, event| match event.id().as_ref() {
            "open" => show_main_window(app),
            "start-daemon" => {
                if let Err(error) = ensure_daemon(app) {
                    eprintln!("replicantd could not be started: {error}");
                }
            }
            "close-to-tray" => {
                let state = app.state::<DesktopState>();
                let mut settings = state.settings.lock().expect("settings lock poisoned");
                settings.close_to_tray = !settings.close_to_tray;
                let checked = settings.close_to_tray;
                drop(settings);
                let _ = close_item.set_checked(checked);
                if let Err(error) = save_settings(&state) {
                    eprintln!("desktop settings could not be saved: {error}");
                }
            }
            "quit" => app.exit(0),
            "quit-all" => {
                stop_managed_daemon(app);
                app.exit(0);
            }
            _ => {}
        })
        .on_tray_icon_event(|tray, event| {
            if matches!(
                event,
                TrayIconEvent::Click {
                    button: MouseButton::Left,
                    button_state: MouseButtonState::Up,
                    ..
                }
            ) {
                show_main_window(tray.app_handle());
            }
        })
        .build(app)?;
    Ok(())
}

fn tray_image() -> Image<'static> {
    let mut rgba = Vec::with_capacity(16 * 16 * 4);
    for y in 0..16 {
        for x in 0..16 {
            let visible = (2..14).contains(&x) && (2..14).contains(&y);
            rgba.extend_from_slice(if visible {
                &[116, 92, 255, 255]
            } else {
                &[0, 0, 0, 0]
            });
        }
    }
    Image::new_owned(rgba, 16, 16)
}

fn show_main_window(app: &AppHandle) {
    if let Some(window) = app.get_webview_window("main") {
        let _ = window.unminimize();
        let _ = window.show();
        let _ = window.set_focus();
    }
}

fn handle_window_event(window: &Window, event: &WindowEvent) {
    let WindowEvent::CloseRequested { api, .. } = event else {
        return;
    };
    let state = window.state::<DesktopState>();
    if state
        .settings
        .lock()
        .expect("settings lock poisoned")
        .close_to_tray
    {
        api.prevent_close();
        let _ = window.hide();
    } else {
        window.app_handle().exit(0);
    }
}

fn ensure_daemon(app: &AppHandle) -> Result<(), Box<dyn std::error::Error>> {
    let address: SocketAddr = DAEMON_BIND.parse()?;
    if daemon_is_running_at(address) {
        return Ok(());
    }

    let state = app.state::<DesktopState>();
    let mut managed = state
        .managed_daemon
        .lock()
        .expect("managed daemon lock poisoned");
    if managed.is_some() {
        return Ok(());
    }

    let mut command = app
        .shell()
        .sidecar("replicantd")?
        .env("REPLICANTD_BIND", DAEMON_BIND);
    if env::var_os("REPLICANT_DB").is_none() {
        command = command.env("REPLICANT_DB", state.data_dir.join("client.sqlite"));
    }
    if env::var_os("REPLICANT_RUNTIME_DB").is_none() {
        command = command.env(
            "REPLICANT_RUNTIME_DB",
            state.data_dir.join("runtime.sqlite"),
        );
    }
    if env::var_os("RS_API_TOKEN").is_none()
        && env::var_os("RS_API_TOKEN_FILE").is_none()
        && state.token_file.is_file()
    {
        command = command.env("RS_API_TOKEN_FILE", &state.token_file);
    }

    let (mut events, child) = command.spawn()?;
    let pid = child.pid();
    *managed = Some(child);
    drop(managed);

    let app = app.clone();
    tauri::async_runtime::spawn(async move {
        while let Some(event) = events.recv().await {
            if let CommandEvent::Terminated(_) = event {
                let state = app.state::<DesktopState>();
                let mut managed = state
                    .managed_daemon
                    .lock()
                    .expect("managed daemon lock poisoned");
                if managed.as_ref().is_some_and(|child| child.pid() == pid) {
                    managed.take();
                }
                break;
            }
        }
    });
    Ok(())
}

fn stop_managed_daemon(app: &AppHandle) {
    let state = app.state::<DesktopState>();
    if let Some(child) = state
        .managed_daemon
        .lock()
        .expect("managed daemon lock poisoned")
        .take()
    {
        let _ = child.kill();
    }
}

fn daemon_is_running_at(address: SocketAddr) -> bool {
    let Ok(mut stream) = TcpStream::connect_timeout(&address, Duration::from_millis(250)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
    let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
    if stream
        .write_all(b"GET /api/health HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = Vec::new();
    stream.read_to_end(&mut response).is_ok() && is_daemon_health_response(&response)
}

fn is_daemon_health_response(response: &[u8]) -> bool {
    response.starts_with(b"HTTP/1.1 200")
        && response
            .windows(b"daemon_version".len())
            .any(|window| window == b"daemon_version")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recognizes_only_successful_daemon_health_responses() {
        assert!(is_daemon_health_response(
            b"HTTP/1.1 200 OK\r\n\r\n{\"daemon_version\":\"0.1.0\"}"
        ));
        assert!(!is_daemon_health_response(
            b"HTTP/1.1 503 Service Unavailable\r\n\r\n{\"daemon_version\":\"0.1.0\"}"
        ));
        assert!(!is_daemon_health_response(b"HTTP/1.1 200 OK\r\n\r\n{}"));
    }

    #[test]
    fn close_to_tray_is_the_safe_default() {
        assert!(DesktopSettings::default().close_to_tray);
    }
}

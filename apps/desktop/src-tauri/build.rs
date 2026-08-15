//! Tauri build-time configuration.

fn main() {
    let icon_dir = std::path::Path::new("icons");
    let icon_path = icon_dir.join("icon.png");
    if !icon_path.exists() {
        std::fs::create_dir_all(icon_dir).expect("create icon directory");
        let file = std::fs::File::create(&icon_path).expect("create application icon");
        let mut encoder = png::Encoder::new(file, 32, 32);
        encoder.set_color(png::ColorType::Rgba);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().expect("write icon header");
        let pixel = [116, 92, 255, 255];
        writer
            .write_image_data(&pixel.repeat(32 * 32))
            .expect("write application icon");
    }
    tauri_build::build();
}

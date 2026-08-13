use crate::math::{project_point, Mat4};

#[derive(Clone)]
pub struct PickMarker {
    pub key: String,
    pub x: f32,
    pub y: f32,
    pub z: f32,
}

pub fn find_marker_at_point(
    markers: &[PickMarker],
    screen_x: f32,
    screen_y: f32,
    hit_radius: f32,
    mvp: &Mat4,
    css_w: f32,
    css_h: f32,
) -> Option<String> {
    if markers.is_empty() || css_w <= 0.0 || css_h <= 0.0 {
        return None;
    }
    let mut best: Option<(String, f32)> = None;
    for marker in markers {
        let Some((px, py)) = project_point(mvp, marker.x, marker.y, marker.z, css_w, css_h) else {
            continue;
        };
        let dist = ((px - screen_x).powi(2) + (py - screen_y).powi(2)).sqrt();
        if dist > hit_radius {
            continue;
        }
        if best.as_ref().is_none_or(|(_, d)| dist < *d) {
            best = Some((marker.key.clone(), dist));
        }
    }
    best.map(|(k, _)| k)
}

use crate::chart_types::{
    GlColoredSphereCenter, GlInfluenceCenter, GlLink, GlPulse, GlSignal, GlStar, TravelRouteLeg,
    Vec3,
};
use js_sys::Date;
use wasm_bindgen::JsValue;

type Rgba = [f32; 4];

const CHEVRON_HALF: f32 = 0.18;
const CHEVRON_WING: f32 = 0.11;
const NUM_CHEVRONS: i32 = 5;
const ANIM_PERIOD: f32 = 1.8;

pub fn build_line_vertices(links: &[GlLink], now_ms: f64) -> Vec<f32> {
    let time_sec = (now_ms / 1000.0) as f32;
    let mut vertices = Vec::new();
    for link in links {
        if link.travel_route {
            let progress = resolve_travel_route_progress(link, now_ms);
            let delta_x = link.to.x - link.from.x;
            let delta_y = link.to.y - link.from.y;
            let delta_z = link.to.z - link.from.z;
            let mid_x = link.from.x + delta_x * progress;
            let mid_y = link.from.y + delta_y * progress;
            let mid_z = link.from.z + delta_z * progress;
            if progress > 0.0 {
                push_line(
                    &mut vertices,
                    [link.from.x, link.from.y, link.from.z],
                    [mid_x, mid_y, mid_z],
                    [0.36, 0.95, 0.88, 0.82],
                );
            }
            if progress < 1.0 {
                push_line(
                    &mut vertices,
                    [mid_x, mid_y, mid_z],
                    [link.to.x, link.to.y, link.to.z],
                    [0.36, 0.95, 0.88, 0.22],
                );
            }
            build_travel_chevrons(&mut vertices, link, progress, time_sec);
            continue;
        }
        if link.relay_coverage_gap {
            push_relay_coverage_gap(&mut vertices, link);
            continue;
        }
        if link.relay {
            push_line(
                &mut vertices,
                [link.from.x, link.from.y, link.from.z],
                [link.to.x, link.to.y, link.to.z],
                [0.4, 0.72, 1.0, 0.88],
            );
            continue;
        }
        if link.exploration_route {
            push_line(
                &mut vertices,
                [link.from.x, link.from.y, link.from.z],
                [link.to.x, link.to.y, link.to.z],
                [0.72, 0.9, 1.0, 0.68],
            );
            continue;
        }
        let dim = if link.secondary { 0.38 } else { 1.0 };
        let color = if link.explored {
            [0.28, 0.65, 1.0, 0.42 * dim]
        } else {
            [0.95, 0.69, 0.31, 0.34 * dim]
        };
        push_line(
            &mut vertices,
            [link.from.x, link.from.y, link.from.z],
            [link.to.x, link.to.y, link.to.z],
            color,
        );
    }
    vertices
}

pub fn build_star_point_vertices(stars: &[GlStar]) -> Vec<f32> {
    let mut vertices = Vec::new();
    for star in stars {
        if star.dimmed {
            push_point(
                &mut vertices,
                star.x,
                star.y,
                star.z,
                hex_to_rgba(star_color(&star.color, &star.spectral_type), 0.14),
                5.5,
            );
            if star.is_megastructure {
                push_megastructure_marker(&mut vertices, star.x, star.y, star.z, false);
            }
            continue;
        }
        if star.current {
            push_point(
                &mut vertices,
                star.x,
                star.y,
                star.z,
                [0.26, 0.83, 0.78, 0.16],
                34.0,
            );
            match star.exploration.as_str() {
                "explored" => {
                    push_star_explored_layers(&mut vertices, star, 28.0, 20.0);
                }
                "partial" => {
                    push_star_partial_layers(&mut vertices, star, 26.0, 20.0);
                }
                _ => {
                    push_point(
                        &mut vertices,
                        star.x,
                        star.y,
                        star.z,
                        inner_star_rgba(star, 1.0),
                        20.0,
                    );
                }
            }
            if star.is_hub {
                push_hub_marker(&mut vertices, star.x, star.y, star.z, true);
            }
            if star.is_megastructure {
                push_megastructure_marker(&mut vertices, star.x, star.y, star.z, true);
            }
            continue;
        }
        match star.exploration.as_str() {
            "explored" => {
                push_star_explored_layers(&mut vertices, star, 28.0, 16.0);
                if star.is_hub {
                    push_hub_marker(&mut vertices, star.x, star.y, star.z, false);
                }
            }
            "partial" => {
                push_star_partial_layers(&mut vertices, star, 26.0, 15.0);
                if star.is_hub {
                    push_hub_marker(&mut vertices, star.x, star.y, star.z, false);
                }
            }
            _ => {
                push_undiscovered_layers(&mut vertices, star, 15.0, 12.0);
                if star.is_hub {
                    push_hub_marker(&mut vertices, star.x, star.y, star.z, false);
                }
            }
        }
        if star.is_megastructure {
            push_megastructure_marker(&mut vertices, star.x, star.y, star.z, false);
        }
    }
    vertices
}

pub fn build_signal_point_vertices(signals: &[GlSignal]) -> Vec<f32> {
    let mut vertices = Vec::new();
    for signal in signals {
        push_point(
            &mut vertices,
            signal.x,
            signal.y,
            signal.z,
            [0.92, 0.38, 1.0, 0.2],
            40.0,
        );
        push_point(
            &mut vertices,
            signal.x,
            signal.y,
            signal.z,
            [0.98, 0.55, 1.0, 0.68],
            26.0,
        );
        push_point(
            &mut vertices,
            signal.x,
            signal.y,
            signal.z,
            [1.0, 0.92, 1.0, 0.96],
            14.0,
        );
    }
    vertices
}

pub fn build_pulse_sphere_geometry(pulses: &[GlPulse]) -> InfluenceSphereGeometry {
    let mut vertices = Vec::new();

    for pulse in pulses {
        let intensity = pulse.intensity.clamp(0.0, 1.0);
        if intensity <= 0.01 {
            continue;
        }
        let radius = PULSE_SPHERE_RADIUS_LY * (0.75 + intensity * 0.25);
        for (scale, alpha) in PULSE_SHELL_LAYERS {
            push_influence_sphere_shell(
                &mut vertices,
                &GlInfluenceCenter {
                    x: pulse.x,
                    y: pulse.y,
                    z: pulse.z,
                },
                radius * scale,
                pulse.r,
                pulse.g,
                pulse.b,
                alpha * intensity,
            );
        }
    }

    let vertex_count = (vertices.len() / 7) as i32;
    InfluenceSphereGeometry {
        vertices,
        vertex_count,
    }
}

pub struct InfluenceSphereGeometry {
    pub vertices: Vec<f32>,
    pub vertex_count: i32,
}

pub const INFLUENCE_SPHERE_RADIUS_LY: f32 = 3.75;
pub const PULSE_SPHERE_RADIUS_LY: f32 = 2.5;
const INFLUENCE_LAT_SEGMENTS: u32 = 12;
const INFLUENCE_LON_SEGMENTS: u32 = 20;
const INFLUENCE_SHELL_LAYERS: [(f32, f32); 4] =
    [(1.0, 0.04), (0.78, 0.028), (0.56, 0.018), (0.34, 0.01)];
const PULSE_SHELL_LAYERS: [(f32, f32); 4] = [(1.0, 0.16), (0.78, 0.11), (0.56, 0.07), (0.34, 0.04)];

pub fn build_colored_sphere_geometry(
    centers: &[GlColoredSphereCenter],
    radius: f32,
) -> InfluenceSphereGeometry {
    let mut vertices = Vec::new();

    for center in centers {
        for (scale, alpha) in INFLUENCE_SHELL_LAYERS {
            push_influence_sphere_shell(
                &mut vertices,
                &GlInfluenceCenter {
                    x: center.x,
                    y: center.y,
                    z: center.z,
                },
                radius * scale,
                center.r,
                center.g,
                center.b,
                alpha,
            );
        }
    }

    let vertex_count = (vertices.len() / 7) as i32;
    InfluenceSphereGeometry {
        vertices,
        vertex_count,
    }
}

pub fn build_influence_sphere_geometry(centers: &[GlInfluenceCenter]) -> InfluenceSphereGeometry {
    let colored: Vec<GlColoredSphereCenter> = centers
        .iter()
        .map(|center| GlColoredSphereCenter {
            x: center.x,
            y: center.y,
            z: center.z,
            r: 0.28,
            g: 0.65,
            b: 1.0,
        })
        .collect();
    build_colored_sphere_geometry(&colored, INFLUENCE_SPHERE_RADIUS_LY)
}

fn push_influence_sphere_shell(
    vertices: &mut Vec<f32>,
    center: &GlInfluenceCenter,
    radius: f32,
    r: f32,
    g: f32,
    blue: f32,
    alpha: f32,
) {
    if radius <= 0.0 || alpha <= 0.0 {
        return;
    }
    let color = [r, g, blue, alpha];

    let lat_n = INFLUENCE_LAT_SEGMENTS;
    let lon_n = INFLUENCE_LON_SEGMENTS;
    let mut positions: Vec<(f32, f32, f32)> = Vec::new();
    let mut ring_start: Vec<usize> = Vec::new();

    for lat in 0..=lat_n {
        ring_start.push(positions.len());
        if lat == 0 {
            positions.push((center.x, center.y + radius, center.z));
            continue;
        }
        if lat == lat_n {
            positions.push((center.x, center.y - radius, center.z));
            continue;
        }
        let theta = (lat as f32 / lat_n as f32) * std::f32::consts::PI;
        let sin_theta = theta.sin();
        let cos_theta = theta.cos();
        for lon in 0..lon_n {
            let phi = (lon as f32 / lon_n as f32) * std::f32::consts::TAU;
            positions.push((
                center.x + radius * sin_theta * phi.cos(),
                center.y + radius * cos_theta,
                center.z + radius * sin_theta * phi.sin(),
            ));
        }
    }

    let north = positions[ring_start[0]];
    let ring_one = ring_start[1];
    for lon in 0..lon_n {
        let a = positions[ring_one + lon as usize];
        let b = positions[ring_one + ((lon + 1) % lon_n) as usize];
        push_influence_triangle(vertices, north, a, b, color);
    }

    for lat in 1..lat_n - 1 {
        let curr = ring_start[lat as usize];
        let next = ring_start[(lat + 1) as usize];
        for lon in 0..lon_n {
            let lon_next = (lon + 1) % lon_n;
            let v00 = positions[curr + lon as usize];
            let v01 = positions[curr + lon_next as usize];
            let v10 = positions[next + lon as usize];
            let v11 = positions[next + lon_next as usize];
            push_influence_triangle(vertices, v00, v01, v11, color);
            push_influence_triangle(vertices, v00, v11, v10, color);
        }
    }

    let south = positions[ring_start[lat_n as usize]];
    let last_ring = ring_start[(lat_n - 1) as usize];
    for lon in 0..lon_n {
        let a = positions[last_ring + lon as usize];
        let b = positions[last_ring + ((lon + 1) % lon_n) as usize];
        push_influence_triangle(vertices, south, b, a, color);
    }
}

fn push_influence_triangle(
    vertices: &mut Vec<f32>,
    a: (f32, f32, f32),
    b: (f32, f32, f32),
    c: (f32, f32, f32),
    color: Rgba,
) {
    push_influence_vertex(vertices, a, color);
    push_influence_vertex(vertices, b, color);
    push_influence_vertex(vertices, c, color);
}

fn push_influence_vertex(vertices: &mut Vec<f32>, position: (f32, f32, f32), color: Rgba) {
    vertices.extend_from_slice(&[
        position.0, position.1, position.2, color[0], color[1], color[2], color[3],
    ]);
}

fn resolve_travel_route_progress(link: &GlLink, now_ms: f64) -> f32 {
    if link.travel_route_legs.is_empty()
        || link.travel_started_at.is_none()
        || link.travel_ends_at.is_none()
    {
        return link.travel_progress.unwrap_or(0.0).clamp(0.0, 1.0);
    }
    let started = link.travel_started_at.as_ref().unwrap();
    let ends = link.travel_ends_at.as_ref().unwrap();
    let Some(active) = resolve_active_route_leg(&link.travel_route_legs, started, ends, now_ms)
    else {
        return 0.0;
    };
    let leg_index = link.travel_route_leg_index.unwrap_or(-1);
    if leg_index < active.index {
        return 1.0;
    }
    if leg_index > active.index {
        return 0.0;
    }
    active.progress.clamp(0.0, 1.0)
}

struct ActiveRouteLeg {
    index: i32,
    progress: f32,
}

fn resolve_active_route_leg(
    route: &[TravelRouteLeg],
    started_at: &str,
    ends_at: &str,
    now_ms: f64,
) -> Option<ActiveRouteLeg> {
    if route.is_empty() {
        return None;
    }
    let start_ms = parse_time_ms(started_at)?;
    let end_ms = parse_time_ms(ends_at)?;
    if end_ms <= start_ms {
        return None;
    }
    let route_seconds: f32 = route.iter().map(|leg| leg.time_seconds.max(0.0)).sum();
    let elapsed_seconds = ((now_ms.min(end_ms) - start_ms) / 1000.0).max(0.0) as f32;
    if route_seconds > 0.0 {
        let mut consumed = 0.0_f32;
        for (index, leg) in route.iter().enumerate() {
            let leg_seconds = leg.time_seconds.max(0.0);
            if leg_seconds <= 0.0 {
                continue;
            }
            if elapsed_seconds <= consumed + leg_seconds {
                return Some(ActiveRouteLeg {
                    index: index as i32,
                    progress: ((elapsed_seconds - consumed) / leg_seconds).clamp(0.0, 1.0),
                });
            }
            consumed += leg_seconds;
        }
        return Some(ActiveRouteLeg {
            index: route.len() as i32 - 1,
            progress: 1.0,
        });
    }
    let overall = ((now_ms - start_ms) / (end_ms - start_ms)).clamp(0.0, 1.0) as f32;
    let leg_index = ((overall * route.len() as f32).floor() as i32).min(route.len() as i32 - 1);
    let leg_progress = (overall * route.len() as f32 - leg_index as f32).clamp(0.0, 1.0);
    Some(ActiveRouteLeg {
        index: leg_index,
        progress: leg_progress,
    })
}

fn parse_time_ms(value: &str) -> Option<f64> {
    let ms = Date::new(&JsValue::from_str(value)).get_time();
    if ms.is_nan() {
        None
    } else {
        Some(ms)
    }
}

fn build_travel_chevrons(vertices: &mut Vec<f32>, link: &GlLink, progress: f32, time_sec: f32) {
    if progress <= 0.0 {
        return;
    }
    let dx = link.to.x - link.from.x;
    let dy = link.to.y - link.from.y;
    let dz = link.to.z - link.from.z;
    let link_len = (dx * dx + dy * dy + dz * dz).sqrt();
    if link_len < 0.001 {
        return;
    }
    let dir_x = dx / link_len;
    let dir_y = dy / link_len;
    let dir_z = dz / link_len;

    let mut perp_x = -dir_z;
    let mut perp_y = 0.0;
    let mut perp_z = dir_x;
    let perp_len = (perp_x * perp_x + perp_y * perp_y + perp_z * perp_z).sqrt();
    if perp_len < 0.001 {
        perp_x = dir_y;
        perp_y = -dir_x;
        perp_z = 0.0;
    } else {
        perp_x /= perp_len;
        perp_z /= perp_len;
    }

    let phase = (time_sec / ANIM_PERIOD) % 1.0;
    for i in 0..NUM_CHEVRONS {
        let t = (i as f32 / NUM_CHEVRONS as f32 + phase) % 1.0;
        if t > progress {
            continue;
        }
        let rel_t = t / progress.max(0.001);
        let alpha = 0.2 + rel_t * 0.75;
        let color = [0.36, 0.95, 0.88, alpha];

        let px = link.from.x + dx * t;
        let py = link.from.y + dy * t;
        let pz = link.from.z + dz * t;
        let tip_x = px + dir_x * CHEVRON_HALF;
        let tip_y = py + dir_y * CHEVRON_HALF;
        let tip_z = pz + dir_z * CHEVRON_HALF;
        let bk_x = px - dir_x * CHEVRON_HALF;
        let bk_y = py - dir_y * CHEVRON_HALF;
        let bk_z = pz - dir_z * CHEVRON_HALF;

        push_line(
            vertices,
            [
                bk_x + perp_x * CHEVRON_WING,
                bk_y + perp_y * CHEVRON_WING,
                bk_z + perp_z * CHEVRON_WING,
            ],
            [tip_x, tip_y, tip_z],
            color,
        );
        push_line(
            vertices,
            [
                bk_x - perp_x * CHEVRON_WING,
                bk_y - perp_y * CHEVRON_WING,
                bk_z - perp_z * CHEVRON_WING,
            ],
            [tip_x, tip_y, tip_z],
            color,
        );
    }
}

fn push_relay_coverage_gap(vertices: &mut Vec<f32>, link: &GlLink) {
    let from = &link.from;
    let to = &link.to;
    let dx = to.x - from.x;
    let dy = to.y - from.y;
    let dz = to.z - from.z;
    let length = (dx * dx + dy * dy + dz * dz).sqrt().max(1.0);
    let dir = Vec3 {
        x: dx / length,
        y: dy / length,
        z: dz / length,
    };
    let reference = if dir.y.abs() < 0.9 {
        Vec3 {
            x: 0.0,
            y: 1.0,
            z: 0.0,
        }
    } else {
        Vec3 {
            x: 1.0,
            y: 0.0,
            z: 0.0,
        }
    };
    let side = normalize_vec(cross(&dir, &reference));
    let up = normalize_vec(cross(&dir, &side));

    for offset in [-0.42_f32, -0.28, -0.16, 0.0, 0.16, 0.28, 0.42] {
        let alpha = if offset == 0.0 {
            1.0
        } else {
            0.16 + (0.42 - offset.abs()) * 0.52
        };
        let color = if offset == 0.0 {
            [1.0, 0.12, 0.04, 1.0]
        } else {
            [1.0, 0.34, 0.04, alpha]
        };
        push_offset_line(vertices, from, to, &side, offset, color);
        if offset != 0.0 {
            push_offset_line(vertices, from, to, &up, offset, color);
        }
    }

    for progress in [0.16_f32, 0.3, 0.44, 0.56, 0.7, 0.84] {
        let center = point_along(from, to, progress);
        push_centered_line(vertices, &center, &side, 0.55, [1.0, 0.82, 0.18, 1.0]);
        push_centered_line(vertices, &center, &up, 0.55, [1.0, 0.38, 0.04, 0.9]);
    }

    let midpoint = point_along(from, to, 0.5);
    push_centered_line(
        vertices,
        &midpoint,
        &normalize_vec(add(&side, &up)),
        1.05,
        [1.0, 0.05, 0.02, 1.0],
    );
    push_centered_line(
        vertices,
        &midpoint,
        &normalize_vec(sub(&side, &up)),
        1.05,
        [1.0, 0.05, 0.02, 1.0],
    );
}

fn push_offset_line(
    vertices: &mut Vec<f32>,
    from: &Vec3,
    to: &Vec3,
    axis: &Vec3,
    offset: f32,
    color: Rgba,
) {
    push_line(
        vertices,
        [
            from.x + axis.x * offset,
            from.y + axis.y * offset,
            from.z + axis.z * offset,
        ],
        [
            to.x + axis.x * offset,
            to.y + axis.y * offset,
            to.z + axis.z * offset,
        ],
        color,
    );
}

fn push_centered_line(
    vertices: &mut Vec<f32>,
    center: &Vec3,
    axis: &Vec3,
    half_length: f32,
    color: Rgba,
) {
    push_line(
        vertices,
        [
            center.x - axis.x * half_length,
            center.y - axis.y * half_length,
            center.z - axis.z * half_length,
        ],
        [
            center.x + axis.x * half_length,
            center.y + axis.y * half_length,
            center.z + axis.z * half_length,
        ],
        color,
    );
}

fn point_along(from: &Vec3, to: &Vec3, progress: f32) -> Vec3 {
    Vec3 {
        x: from.x + (to.x - from.x) * progress,
        y: from.y + (to.y - from.y) * progress,
        z: from.z + (to.z - from.z) * progress,
    }
}

fn cross(left: &Vec3, right: &Vec3) -> Vec3 {
    Vec3 {
        x: left.y * right.z - left.z * right.y,
        y: left.z * right.x - left.x * right.z,
        z: left.x * right.y - left.y * right.x,
    }
}

fn normalize_vec(vector: Vec3) -> Vec3 {
    let length = (vector.x * vector.x + vector.y * vector.y + vector.z * vector.z)
        .sqrt()
        .max(1.0);
    Vec3 {
        x: vector.x / length,
        y: vector.y / length,
        z: vector.z / length,
    }
}

fn add(left: &Vec3, right: &Vec3) -> Vec3 {
    Vec3 {
        x: left.x + right.x,
        y: left.y + right.y,
        z: left.z + right.z,
    }
}

fn sub(left: &Vec3, right: &Vec3) -> Vec3 {
    Vec3 {
        x: left.x - right.x,
        y: left.y - right.y,
        z: left.z - right.z,
    }
}

fn push_hub_marker(vertices: &mut Vec<f32>, x: f32, y: f32, z: f32, is_current: bool) {
    push_point(
        vertices,
        x,
        y,
        z,
        [0.1, 0.96, 0.86, 0.24],
        if is_current { 46.0 } else { 42.0 },
    );
    push_point(
        vertices,
        x,
        y,
        z,
        [0.98, 0.82, 0.34, 0.36],
        if is_current { 34.0 } else { 30.0 },
    );
    push_point(
        vertices,
        x,
        y,
        z,
        [1.0, 0.97, 0.78, 0.95],
        if is_current { 20.0 } else { 18.0 },
    );
}

fn push_megastructure_marker(vertices: &mut Vec<f32>, x: f32, y: f32, z: f32, is_current: bool) {
    push_point(
        vertices,
        x,
        y,
        z,
        [0.74, 0.48, 1.0, 0.2],
        if is_current { 56.0 } else { 50.0 },
    );
    push_point(
        vertices,
        x,
        y,
        z,
        [0.91, 0.76, 1.0, 0.5],
        if is_current { 40.0 } else { 36.0 },
    );
}

fn push_line(vertices: &mut Vec<f32>, from: [f32; 3], to: [f32; 3], color: Rgba) {
    vertices.extend_from_slice(&[
        from[0], from[1], from[2], color[0], color[1], color[2], color[3],
    ]);
    vertices.extend_from_slice(&[to[0], to[1], to[2], color[0], color[1], color[2], color[3]]);
}

fn push_point(vertices: &mut Vec<f32>, x: f32, y: f32, z: f32, color: Rgba, size: f32) {
    vertices.extend_from_slice(&[x, y, z, color[0], color[1], color[2], color[3], size]);
}

fn push_undiscovered_layers(vertices: &mut Vec<f32>, star: &GlStar, outer: f32, inner: f32) {
    let spectral = spectral_star_rgba(star, 0.9);
    push_point(
        vertices,
        star.x,
        star.y,
        star.z,
        [spectral[0], spectral[1], spectral[2], 0.22],
        outer,
    );
    push_point(vertices, star.x, star.y, star.z, spectral, inner);
}

fn push_star_explored_layers(vertices: &mut Vec<f32>, star: &GlStar, outer: f32, inner: f32) {
    if star.is_relay {
        push_point(
            vertices,
            star.x,
            star.y,
            star.z,
            [0.4, 0.72, 1.0, 0.24],
            outer,
        );
        push_point(
            vertices,
            star.x,
            star.y,
            star.z,
            relay_star_rgba(star),
            inner,
        );
        return;
    }
    let spectral = spectral_star_rgba(star, 1.0);
    push_point(
        vertices,
        star.x,
        star.y,
        star.z,
        [spectral[0], spectral[1], spectral[2], 0.14],
        outer,
    );
    push_point(vertices, star.x, star.y, star.z, spectral, inner);
}

fn push_star_partial_layers(vertices: &mut Vec<f32>, star: &GlStar, outer: f32, inner: f32) {
    if star.is_relay {
        push_point(
            vertices,
            star.x,
            star.y,
            star.z,
            [0.4, 0.72, 1.0, 0.2],
            outer,
        );
        push_point(
            vertices,
            star.x,
            star.y,
            star.z,
            relay_star_rgba(star),
            inner,
        );
        return;
    }
    let spectral = spectral_star_rgba(star, 0.92);
    push_point(
        vertices,
        star.x,
        star.y,
        star.z,
        [spectral[0], spectral[1], spectral[2], 0.12],
        outer,
    );
    push_point(vertices, star.x, star.y, star.z, spectral, inner);
}

fn inner_star_rgba(star: &GlStar, alpha: f32) -> Rgba {
    if star.is_relay {
        let relay = relay_star_rgba(star);
        return [relay[0], relay[1], relay[2], alpha];
    }
    spectral_star_rgba(star, alpha)
}

fn spectral_star_rgba(star: &GlStar, alpha: f32) -> Rgba {
    hex_to_rgba(star_color(&star.color, &star.spectral_type), alpha)
}

fn relay_star_rgba(star: &GlStar) -> Rgba {
    let base = hex_to_rgba(star_color(&star.color, &star.spectral_type), 1.0);
    let relay = [0.4, 0.72, 1.0, 0.96];
    [
        base[0] * 0.3 + relay[0] * 0.7,
        base[1] * 0.3 + relay[1] * 0.7,
        base[2] * 0.3 + relay[2] * 0.7,
        relay[3],
    ]
}

fn star_color(color: &str, spectral_type: &str) -> &'static str {
    let n = color.to_lowercase();
    if n.contains("blue") {
        return "#8ec5ff";
    }
    if n.contains("white") {
        return "#eef6ff";
    }
    if n.contains("yellow") {
        return "#ffd76a";
    }
    if n.contains("orange") {
        return "#ffad66";
    }
    if n.contains("red") {
        return "#ff7b6b";
    }
    spectral_class_color(spectral_type)
}

fn spectral_class_color(spectral_type: &str) -> &'static str {
    match spectral_type.trim().chars().next().unwrap_or(' ') {
        'O' | 'o' | 'B' | 'b' => "#8ec5ff",
        'A' | 'a' => "#eef6ff",
        'F' | 'f' => "#fff4e0",
        'G' | 'g' => "#ffd76a",
        'K' | 'k' => "#ffad66",
        'M' | 'm' => "#ff7b6b",
        _ => "#7ef0d3",
    }
}

fn hex_to_rgba(hex: &str, alpha: f32) -> Rgba {
    let trimmed = hex.trim_start_matches('#');
    if trimmed.len() < 6 {
        return [1.0, 1.0, 1.0, alpha];
    }
    let r = u8::from_str_radix(&trimmed[0..2], 16).unwrap_or(255) as f32 / 255.0;
    let g = u8::from_str_radix(&trimmed[2..4], 16).unwrap_or(255) as f32 / 255.0;
    let b = u8::from_str_radix(&trimmed[4..6], 16).unwrap_or(255) as f32 / 255.0;
    [r, g, b, alpha]
}

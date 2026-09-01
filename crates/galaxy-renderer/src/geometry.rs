pub const MAX_GALAXY_LINK_DISTANCE_LY: f32 = 7.5;
const SPHERE_SEGMENTS: u32 = 32;
pub const MAX_SPHERE_RINGS: usize = 12;

pub fn build_galaxy_plane_verts(radius: f32, segments: u32) -> Vec<f32> {
    let mut verts = Vec::with_capacity(((segments + 2) * 7) as usize);
    let (cr, cg, cb) = (0.2_f32, 0.72, 0.68);
    verts.extend_from_slice(&[0.0, 0.0, 0.0, cr, cg, cb, 0.09]);
    for i in 0..=segments {
        let a = (i as f32 / segments as f32) * std::f32::consts::TAU;
        verts.extend_from_slice(&[radius * a.cos(), 0.0, radius * a.sin(), cr, cg, cb, 0.0]);
    }
    verts
}

/// Static sphere template: each line endpoint is (ring_index, angle).
pub fn build_sphere_line_template(ring_count: usize, segments: u32) -> (Vec<f32>, i32) {
    let n = segments.max(3);
    let mut attrs = Vec::with_capacity(ring_count * n as usize * 4);
    for ring in 0..ring_count {
        for i in 0..n {
            let a0 = (i as f32 / n as f32) * std::f32::consts::TAU;
            let a1 = ((i + 1) as f32 / n as f32) * std::f32::consts::TAU;
            attrs.extend_from_slice(&[ring as f32, a0, ring as f32, a1]);
        }
    }
    let vertex_count = (ring_count as i32) * (n as i32) * 2;
    (attrs, vertex_count)
}

pub fn sphere_template_segments() -> u32 {
    SPHERE_SEGMENTS
}

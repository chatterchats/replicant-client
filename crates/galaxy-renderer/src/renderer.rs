use crate::camera::{Camera, DragMode, DragState};
use crate::chart_geometry::{
    build_colored_sphere_geometry, build_influence_sphere_geometry, build_line_vertices, build_pulse_sphere_geometry,
    build_signal_point_vertices, build_star_point_vertices, INFLUENCE_SPHERE_RADIUS_LY,
};
use crate::chart_types::{parse_colored_sphere_centers, parse_influence_centers, parse_links, parse_pulses, parse_signals, parse_stars};
use crate::geometry::{build_galaxy_plane_verts, build_sphere_line_template, sphere_template_segments, MAX_GALAXY_LINK_DISTANCE_LY, MAX_SPHERE_RINGS};
use crate::gl::{self, GlPrograms};
use crate::math::build_mvp;
use crate::pick::{find_marker_at_point, PickMarker};
use glow::HasContext;
use std::collections::HashMap;
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::HtmlCanvasElement;

const MAX_CENSUS_CACHE_ENTRIES: usize = 12;

pub struct GalaxyRendererInner {
    canvas: HtmlCanvasElement,
    gl: glow::Context,
    programs: GlPrograms,
    static_line_buf: glow::Buffer,
    relay_line_buf: glow::Buffer,
    highlight_line_buf: glow::Buffer,
    travel_line_buf: glow::Buffer,
    sphere_buf: glow::Buffer,
    plane_buf: glow::Buffer,
    point_buf: glow::Buffer,
    pulse_sphere_buf: glow::Buffer,
    life_sphere_buf: glow::Buffer,
    device_sphere_buf: glow::Buffer,
    influence_buf: glow::Buffer,
    static_line_count: i32,
    relay_line_count: i32,
    highlight_line_count: i32,
    travel_line_count: i32,
    point_count: i32,
    pulse_sphere_vertex_count: i32,
    life_sphere_vertex_count: i32,
    device_sphere_vertex_count: i32,
    influence_vertex_count: i32,
    sphere_line_count: i32,
    sphere_radii: Vec<f32>,
    camera: Camera,
    camera_dirty: bool,
    css_width: f32,
    css_height: f32,
    pixel_width: i32,
    pixel_height: i32,
    pixel_ratio: f32,
    mvp: [f32; 16],
    stars: Vec<PickMarker>,
    signals: Vec<PickMarker>,
    cached_plane_verts: Vec<f32>,
    cached_plane_radius: f32,
    drag: Option<DragState>,
    census_line_cache: HashMap<String, Vec<f32>>,
    census_cache_order: Vec<String>,
}

impl GalaxyRendererInner {
    pub fn new(canvas: HtmlCanvasElement) -> Result<Self, String> {
        let context = canvas
            .get_context("webgl")
            .map_err(|_| "get_context failed")?
            .ok_or("WebGL unavailable")?;
        let webgl: web_sys::WebGlRenderingContext = context
            .dyn_into()
            .map_err(|_| "not a WebGL context")?;
        let gl = glow::Context::from_webgl1_context(webgl);
        let programs = gl::create_programs(&gl)?;

        unsafe {
            gl.enable(glow::BLEND);
            gl.blend_func(glow::SRC_ALPHA, glow::ONE_MINUS_SRC_ALPHA);
        }

        let create_buf = || unsafe { gl.create_buffer().map_err(|e| e.to_string()) };

        Ok(Self {
            canvas,
            static_line_buf: create_buf()?,
            relay_line_buf: create_buf()?,
            highlight_line_buf: create_buf()?,
            travel_line_buf: create_buf()?,
            sphere_buf: create_buf()?,
            plane_buf: create_buf()?,
            point_buf: create_buf()?,
            pulse_sphere_buf: create_buf()?,
            life_sphere_buf: create_buf()?,
            device_sphere_buf: create_buf()?,
            influence_buf: create_buf()?,
            gl,
            programs,
            static_line_count: 0,
            relay_line_count: 0,
            highlight_line_count: 0,
            travel_line_count: 0,
            point_count: 0,
            pulse_sphere_vertex_count: 0,
            life_sphere_vertex_count: 0,
            device_sphere_vertex_count: 0,
            influence_vertex_count: 0,
            sphere_radii: Vec::new(),
            camera: Camera::default(),
            camera_dirty: true,
            css_width: 0.0,
            css_height: 0.0,
            pixel_width: 0,
            pixel_height: 0,
            pixel_ratio: 1.0,
            mvp: [0.0; 16],
            stars: Vec::new(),
            signals: Vec::new(),
            cached_plane_verts: Vec::new(),
            cached_plane_radius: -1.0,
            sphere_line_count: 0,
            drag: None,
            census_line_cache: HashMap::new(),
            census_cache_order: Vec::new(),
        })
    }

    fn upload_sphere_template(&mut self) {
        let ring_count = self.sphere_radii.len();
        if ring_count == 0 {
            self.sphere_line_count = 0;
            return;
        }
        let (template, count) = build_sphere_line_template(ring_count, sphere_template_segments());
        gl::upload_buffer(&self.gl, self.sphere_buf, &template, glow::STATIC_DRAW);
        self.sphere_line_count = count;
    }

    fn cached_census_lines(&mut self, cache_key: &str, links_json: &str) -> Vec<f32> {
        if self.census_line_cache.contains_key(cache_key) {
            if let Some(pos) = self.census_cache_order.iter().position(|key| key == cache_key) {
                self.census_cache_order.remove(pos);
            }
            self.census_cache_order.push(cache_key.to_string());
            return self.census_line_cache.get(cache_key).unwrap().clone();
        }
        let links = parse_links(links_json);
        let verts = build_line_vertices(&links, 0.0);
        self.census_line_cache.insert(cache_key.to_string(), verts.clone());
        self.census_cache_order.push(cache_key.to_string());
        if self.census_cache_order.len() > MAX_CENSUS_CACHE_ENTRIES {
            if let Some(oldest) = self.census_cache_order.first().cloned() {
                self.census_cache_order.remove(0);
                self.census_line_cache.remove(&oldest);
            }
        }
        verts
    }

    pub fn resize(&mut self, css_w: f32, css_h: f32) {
        self.css_width = css_w.max(1.0);
        self.css_height = css_h.max(1.0);
        self.pixel_ratio = web_sys::window()
            .map(|w| w.device_pixel_ratio() as f32)
            .unwrap_or(1.0)
            .max(1.0);
        let pw = (self.css_width * self.pixel_ratio).round().max(1.0) as i32;
        let ph = (self.css_height * self.pixel_ratio).round().max(1.0) as i32;
        if pw == self.pixel_width && ph == self.pixel_height {
            return;
        }
        self.pixel_width = pw;
        self.pixel_height = ph;
        self.canvas.set_width(pw as u32);
        self.canvas.set_height(ph as u32);
        unsafe {
            self.gl.viewport(0, 0, pw, ph);
        }
        self.camera_dirty = true;
    }

    pub fn set_sphere_radii(&mut self, radii: &[f32]) {
        self.sphere_radii = radii.iter().copied().take(MAX_SPHERE_RINGS).collect();
        self.cached_plane_radius = -1.0;
        self.ensure_plane_cache();
        self.upload_sphere_template();
        self.camera_dirty = true;
    }

    pub fn set_census_geometry(&mut self, cache_key: &str, stars_json: &str, links_json: &str, signals_json: &str) {
        let stars = parse_stars(stars_json);
        let signals = parse_signals(signals_json);
        let line_verts = self.cached_census_lines(cache_key, links_json);
        let mut point_verts = build_star_point_vertices(&stars);
        point_verts.extend(build_signal_point_vertices(&signals));

        gl::upload_buffer(&self.gl, self.static_line_buf, &line_verts, glow::STATIC_DRAW);
        gl::upload_buffer(&self.gl, self.point_buf, &point_verts, glow::STATIC_DRAW);
        self.static_line_count = (line_verts.len() / 7) as i32;
        self.point_count = (point_verts.len() / 8) as i32;
        self.stars = stars
            .into_iter()
            .map(|star| PickMarker {
                key: star.designation,
                x: star.x,
                y: star.y,
                z: star.z,
            })
            .collect();
        self.signals = signals
            .into_iter()
            .map(|signal| PickMarker {
                key: signal.key,
                x: signal.x,
                y: signal.y,
                z: signal.z,
            })
            .collect();
    }

    pub fn set_relay_geometry(&mut self, links_json: &str) {
        let links = parse_links(links_json);
        let line_verts = build_line_vertices(&links, 0.0);
        gl::upload_buffer(&self.gl, self.relay_line_buf, &line_verts, glow::DYNAMIC_DRAW);
        self.relay_line_count = (line_verts.len() / 7) as i32;
    }

    pub fn set_highlight_geometry(&mut self, links_json: &str) {
        let links = parse_links(links_json);
        let line_verts = build_line_vertices(&links, 0.0);
        gl::upload_buffer(&self.gl, self.highlight_line_buf, &line_verts, glow::DYNAMIC_DRAW);
        self.highlight_line_count = (line_verts.len() / 7) as i32;
    }

    pub fn set_travel_geometry(&mut self, links_json: &str, now_ms: f64) {
        let links = parse_links(links_json);
        let line_verts = build_line_vertices(&links, now_ms);
        gl::upload_buffer(&self.gl, self.travel_line_buf, &line_verts, glow::DYNAMIC_DRAW);
        self.travel_line_count = (line_verts.len() / 7) as i32;
    }

    pub fn set_pulse_geometry(&mut self, pulses_json: &str) {
        let pulses = parse_pulses(pulses_json);
        let geometry = build_pulse_sphere_geometry(&pulses);
        gl::upload_buffer(&self.gl, self.pulse_sphere_buf, &geometry.vertices, glow::DYNAMIC_DRAW);
        self.pulse_sphere_vertex_count = geometry.vertex_count;
    }

    pub fn set_life_sphere_geometry(&mut self, centers_json: &str) {
        self.life_sphere_vertex_count =
            self.upload_colored_sphere_geometry(self.life_sphere_buf, centers_json);
    }

    pub fn set_device_sphere_geometry(&mut self, centers_json: &str) {
        self.device_sphere_vertex_count =
            self.upload_colored_sphere_geometry(self.device_sphere_buf, centers_json);
    }

    fn upload_colored_sphere_geometry(&mut self, buffer: glow::Buffer, centers_json: &str) -> i32 {
        let centers = parse_colored_sphere_centers(centers_json);
        let geometry = build_colored_sphere_geometry(&centers, INFLUENCE_SPHERE_RADIUS_LY);
        gl::upload_buffer(&self.gl, buffer, &geometry.vertices, glow::DYNAMIC_DRAW);
        geometry.vertex_count
    }

    pub fn set_influence_geometry(&mut self, centers_json: &str) {
        let centers = parse_influence_centers(centers_json);
        let geometry = build_influence_sphere_geometry(&centers);
        gl::upload_buffer(
            &self.gl,
            self.influence_buf,
            &geometry.vertices,
            glow::DYNAMIC_DRAW,
        );
        self.influence_vertex_count = geometry.vertex_count;
    }

    pub fn set_camera(&mut self, theta: f32, phi: f32, distance: f32, tx: f32, ty: f32, tz: f32) {
        let next = Camera {
            theta,
            phi,
            distance,
            target_x: tx,
            target_y: ty,
            target_z: tz,
        };
        if next != self.camera {
            self.camera = next;
            self.camera_dirty = true;
        }
    }

    pub fn get_camera(&self) -> Camera {
        self.camera
    }

    pub fn pointer_down(&mut self, x: f32, y: f32, button: i32, shift: bool) -> bool {
        if button != 0 && button != 1 {
            return false;
        }
        let mode = if button == 1 || shift {
            DragMode::Pan
        } else {
            DragMode::Rotate
        };
        self.drag = Some(DragState {
            start_x: x,
            start_y: y,
            start_theta: self.camera.theta,
            start_phi: self.camera.phi,
            start_tx: self.camera.target_x,
            start_ty: self.camera.target_y,
            start_tz: self.camera.target_z,
            mode,
            moved: false,
        });
        true
    }

    pub fn pointer_move(&mut self, x: f32, y: f32) -> bool {
        let Some(mut drag) = self.drag else {
            return false;
        };
        let dx = x - drag.start_x;
        let dy = y - drag.start_y;
        if !drag.moved && (dx * dx + dy * dy).sqrt() > 4.0 {
            drag.moved = true;
        }
        if !drag.moved {
            self.drag = Some(drag);
            return false;
        }
        self.camera.apply_drag(&drag, x, y);
        self.drag = Some(drag);
        self.camera_dirty = true;
        true
    }

    pub fn pointer_up(&mut self, x: f32, y: f32) -> Option<String> {
        let drag = self.drag.take()?;
        if drag.moved {
            None
        } else {
            self.pick_star(x, y)
        }
    }

    pub fn wheel(&mut self, delta_y: f32) {
        self.camera.zoom(delta_y);
        self.camera_dirty = true;
    }

    pub fn pick_star(&self, screen_x: f32, screen_y: f32) -> Option<String> {
        find_marker_at_point(&self.stars, screen_x, screen_y, 20.0, &self.mvp, self.css_width, self.css_height)
    }

    pub fn pick_signal(&self, screen_x: f32, screen_y: f32) -> Option<String> {
        find_marker_at_point(&self.signals, screen_x, screen_y, 24.0, &self.mvp, self.css_width, self.css_height)
    }

    fn ensure_plane_cache(&mut self) {
        let outer_r = self.sphere_radii.last().copied().unwrap_or(MAX_GALAXY_LINK_DISTANCE_LY);
        if (self.cached_plane_radius - outer_r).abs() > f32::EPSILON {
            self.cached_plane_verts = build_galaxy_plane_verts(outer_r, 128);
            self.cached_plane_radius = outer_r;
            gl::upload_buffer(&self.gl, self.plane_buf, &self.cached_plane_verts, glow::STATIC_DRAW);
        }
    }

    fn update_mvp(&mut self) {
        let aspect = if self.pixel_height > 0 {
            self.pixel_width as f32 / self.pixel_height as f32
        } else {
            1.0
        };
        self.mvp = build_mvp(
            self.camera.theta,
            self.camera.phi,
            self.camera.distance,
            self.camera.target_x,
            self.camera.target_y,
            self.camera.target_z,
            aspect,
        );
    }

    pub fn render(&mut self) {
        if self.pixel_width <= 0 || self.pixel_height <= 0 {
            return;
        }

        if self.camera_dirty {
            self.update_mvp();
            self.camera_dirty = false;
        }

        let zoom_scale = 20.0 / self.camera.distance.max(0.01);
        let plane_count = (self.cached_plane_verts.len() / 7) as i32;

        unsafe {
            self.gl.clear_color(0.0, 0.0, 0.0, 0.0);
            self.gl.clear(glow::COLOR_BUFFER_BIT);
        }

        gl::draw_triangle_fan(&self.gl, &self.programs, self.plane_buf, &self.mvp, 0, plane_count);
        gl::draw_glow_triangles(
            &self.gl,
            &self.programs,
            self.influence_buf,
            &self.mvp,
            self.influence_vertex_count,
        );
        gl::draw_glow_triangles(
            &self.gl,
            &self.programs,
            self.life_sphere_buf,
            &self.mvp,
            self.life_sphere_vertex_count,
        );
        gl::draw_glow_triangles(
            &self.gl,
            &self.programs,
            self.device_sphere_buf,
            &self.mvp,
            self.device_sphere_vertex_count,
        );
        gl::draw_sphere_lines(
            &self.gl,
            &self.programs,
            self.sphere_buf,
            &self.mvp,
            [
                self.camera.target_x,
                self.camera.target_y,
                self.camera.target_z,
            ],
            self.camera.distance,
            self.camera.theta,
            self.camera.phi,
            &self.sphere_radii,
            self.sphere_line_count,
        );
        gl::draw_lines(&self.gl, &self.programs, self.static_line_buf, &self.mvp, self.static_line_count);
        gl::draw_lines(&self.gl, &self.programs, self.relay_line_buf, &self.mvp, self.relay_line_count);
        gl::draw_glow_lines(
            &self.gl,
            &self.programs,
            self.highlight_line_buf,
            &self.mvp,
            self.highlight_line_count,
        );
        gl::draw_lines(&self.gl, &self.programs, self.travel_line_buf, &self.mvp, self.travel_line_count);
        gl::draw_points(
            &self.gl,
            &self.programs,
            self.point_buf,
            &self.mvp,
            self.pixel_ratio,
            zoom_scale,
            self.point_count,
        );
        if self.pulse_sphere_vertex_count > 0 {
            gl::draw_glow_triangles(
                &self.gl,
                &self.programs,
                self.pulse_sphere_buf,
                &self.mvp,
                self.pulse_sphere_vertex_count,
            );
        }
    }
}

#[wasm_bindgen]
pub struct GalaxyRenderer {
    inner: GalaxyRendererInner,
}

#[wasm_bindgen]
impl GalaxyRenderer {
    #[wasm_bindgen(constructor)]
    pub fn new(canvas: HtmlCanvasElement) -> Result<GalaxyRenderer, JsValue> {
        GalaxyRendererInner::new(canvas)
            .map(|inner| GalaxyRenderer { inner })
            .map_err(|e| JsValue::from_str(&e))
    }

    pub fn resize(&mut self, css_w: f32, css_h: f32) {
        self.inner.resize(css_w, css_h);
    }

    pub fn set_sphere_radii(&mut self, radii: &[f32]) {
        self.inner.set_sphere_radii(radii);
    }

    pub fn set_census_geometry(&mut self, cache_key: &str, stars_json: &str, links_json: &str, signals_json: &str) {
        self.inner.set_census_geometry(cache_key, stars_json, links_json, signals_json);
    }

    pub fn set_relay_geometry(&mut self, links_json: &str) {
        self.inner.set_relay_geometry(links_json);
    }

    pub fn set_highlight_geometry(&mut self, links_json: &str) {
        self.inner.set_highlight_geometry(links_json);
    }

    pub fn set_travel_geometry(&mut self, links_json: &str, now_ms: f64) {
        self.inner.set_travel_geometry(links_json, now_ms);
    }

    pub fn set_pulse_geometry(&mut self, pulses_json: &str) {
        self.inner.set_pulse_geometry(pulses_json);
    }

    pub fn set_life_sphere_geometry(&mut self, centers_json: &str) {
        self.inner.set_life_sphere_geometry(centers_json);
    }

    pub fn set_device_sphere_geometry(&mut self, centers_json: &str) {
        self.inner.set_device_sphere_geometry(centers_json);
    }

    pub fn set_influence_geometry(&mut self, centers_json: &str) {
        self.inner.set_influence_geometry(centers_json);
    }

    pub fn set_camera(&mut self, theta: f32, phi: f32, distance: f32, tx: f32, ty: f32, tz: f32) {
        self.inner.set_camera(theta, phi, distance, tx, ty, tz);
    }

    pub fn get_camera(&self) -> JsValue {
        let cam = self.inner.get_camera();
        serde_wasm_bindgen::to_value(&serde_json::json!({
            "theta": cam.theta,
            "phi": cam.phi,
            "distance": cam.distance,
            "targetX": cam.target_x,
            "targetY": cam.target_y,
            "targetZ": cam.target_z,
        }))
        .unwrap_or(JsValue::NULL)
    }

    pub fn pointer_down(&mut self, x: f32, y: f32, button: i32, shift: bool) -> bool {
        self.inner.pointer_down(x, y, button, shift)
    }

    pub fn pointer_move(&mut self, x: f32, y: f32) -> bool {
        self.inner.pointer_move(x, y)
    }

    pub fn pointer_up(&mut self, x: f32, y: f32) -> Option<String> {
        self.inner.pointer_up(x, y)
    }

    pub fn wheel(&mut self, delta_y: f32) {
        self.inner.wheel(delta_y);
    }

    pub fn pick_star(&self, screen_x: f32, screen_y: f32) -> Option<String> {
        self.inner.pick_star(screen_x, screen_y)
    }

    pub fn pick_signal(&self, screen_x: f32, screen_y: f32) -> Option<String> {
        self.inner.pick_signal(screen_x, screen_y)
    }

    pub fn render(&mut self) {
        self.inner.render();
    }

    pub fn camera_theta(&self) -> f32 {
        self.inner.camera.theta
    }

    pub fn camera_phi(&self) -> f32 {
        self.inner.camera.phi
    }

    pub fn camera_distance(&self) -> f32 {
        self.inner.camera.distance
    }

    pub fn camera_target_x(&self) -> f32 {
        self.inner.camera.target_x
    }

    pub fn camera_target_y(&self) -> f32 {
        self.inner.camera.target_y
    }

    pub fn camera_target_z(&self) -> f32 {
        self.inner.camera.target_z
    }
}

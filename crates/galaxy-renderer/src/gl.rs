use glow::HasContext;

const LINE_VERTEX_SRC: &str = r#"
attribute vec3 a_world;
attribute vec4 a_color;
uniform mat4 u_mvp;
varying vec4 v_color;
void main() {
  gl_Position = u_mvp * vec4(a_world, 1.0);
  v_color = a_color;
}"#;

const LINE_FRAG_SRC: &str = r#"
precision mediump float;
varying vec4 v_color;
void main() { gl_FragColor = v_color; }"#;

const POINT_VERTEX_SRC: &str = r#"
attribute vec3 a_world;
attribute vec4 a_color;
attribute float a_size;
uniform mat4 u_mvp;
uniform float u_pixel_ratio;
uniform float u_zoom_scale;
varying vec4 v_color;
varying float v_depth_norm;
void main() {
  gl_Position = u_mvp * vec4(a_world, 1.0);
  gl_PointSize = min(50.0, max(1.0, a_size * u_zoom_scale)) * u_pixel_ratio;
  v_color = a_color;
  v_depth_norm = gl_Position.w * u_zoom_scale / 20.0;
}"#;

const POINT_FRAG_SRC: &str = r#"
precision mediump float;
varying vec4 v_color;
varying float v_depth_norm;
void main() {
  vec2 d = gl_PointCoord - vec2(0.5);
  float dist = length(d);
  if (dist > 0.5) discard;
  float depth_fade = clamp(pow(1.0 / max(0.01, v_depth_norm), 1.5), 0.12, 1.0);
  float edge = smoothstep(0.5, 0.44, dist);
  gl_FragColor = vec4(v_color.rgb, edge * v_color.a * depth_fade);
}"#;

const SPHERE_VERTEX_SRC: &str = r#"
attribute float a_ring;
attribute float a_angle;
uniform mat4 u_mvp;
uniform vec3 u_target;
uniform float u_distance;
uniform float u_theta;
uniform float u_phi;
uniform float u_r0;
uniform float u_r1;
uniform float u_r2;
uniform float u_r3;
uniform float u_r4;
uniform float u_r5;
uniform float u_r6;
uniform float u_r7;
uniform float u_r8;
uniform float u_r9;
uniform float u_r10;
uniform float u_r11;
uniform int u_ring_count;
varying vec4 v_color;

float ring_radius(int idx) {
  if (idx == 0) return u_r0;
  if (idx == 1) return u_r1;
  if (idx == 2) return u_r2;
  if (idx == 3) return u_r3;
  if (idx == 4) return u_r4;
  if (idx == 5) return u_r5;
  if (idx == 6) return u_r6;
  if (idx == 7) return u_r7;
  if (idx == 8) return u_r8;
  if (idx == 9) return u_r9;
  if (idx == 10) return u_r10;
  return u_r11;
}

void main() {
  int ring = int(a_ring + 0.5);
  if (ring < 0 || ring >= u_ring_count) {
    gl_Position = vec4(2.0, 2.0, 2.0, 1.0);
    v_color = vec4(0.0);
    return;
  }
  float r = ring_radius(ring);
  if (u_distance <= r) {
    gl_Position = vec4(2.0, 2.0, 2.0, 1.0);
    v_color = vec4(0.0);
    return;
  }
  float d = u_distance;
  float r_eff = (r * d) / sqrt(d * d - r * r);
  float rx = cos(u_theta);
  float rz = -sin(u_theta);
  float ux = -sin(u_theta) * sin(u_phi);
  float uy = cos(u_phi);
  float uz = -cos(u_theta) * sin(u_phi);
  float c = cos(a_angle);
  float s = sin(a_angle);
  vec3 world = u_target + r_eff * (vec3(rx, 0.0, rz) * c + vec3(ux, uy, uz) * s);
  float alpha = 0.28 / (1.0 + float(ring) * 0.38);
  v_color = vec4(0.25, 0.83, 0.79, alpha);
  gl_Position = u_mvp * vec4(world, 1.0);
}"#;

const SPHERE_FRAG_SRC: &str = r#"
precision mediump float;
varying vec4 v_color;
void main() { gl_FragColor = v_color; }"#;

pub struct GlPrograms {
    pub line_program: glow::Program,
    pub point_program: glow::Program,
    pub sphere_program: glow::Program,
    pub l_pos: u32,
    pub l_col: u32,
    pub p_pos: u32,
    pub p_col: u32,
    pub p_size: u32,
    pub s_ring: u32,
    pub s_angle: u32,
    pub l_mvp: glow::UniformLocation,
    pub p_mvp: glow::UniformLocation,
    pub p_pixel_ratio: glow::UniformLocation,
    pub p_zoom_scale: glow::UniformLocation,
    pub s_mvp: glow::UniformLocation,
    pub s_target: glow::UniformLocation,
    pub s_distance: glow::UniformLocation,
    pub s_theta: glow::UniformLocation,
    pub s_phi: glow::UniformLocation,
    pub s_ring_count: glow::UniformLocation,
    pub s_radii: [glow::UniformLocation; 12],
}

pub fn create_programs(gl: &glow::Context) -> Result<GlPrograms, String> {
    let line_program = create_program(gl, LINE_VERTEX_SRC, LINE_FRAG_SRC)?;
    let point_program = create_program(gl, POINT_VERTEX_SRC, POINT_FRAG_SRC)?;
    let sphere_program = create_program(gl, SPHERE_VERTEX_SRC, SPHERE_FRAG_SRC)?;
    unsafe {
        let s_radii = [
            gl.get_uniform_location(sphere_program, "u_r0").ok_or("missing u_r0")?,
            gl.get_uniform_location(sphere_program, "u_r1").ok_or("missing u_r1")?,
            gl.get_uniform_location(sphere_program, "u_r2").ok_or("missing u_r2")?,
            gl.get_uniform_location(sphere_program, "u_r3").ok_or("missing u_r3")?,
            gl.get_uniform_location(sphere_program, "u_r4").ok_or("missing u_r4")?,
            gl.get_uniform_location(sphere_program, "u_r5").ok_or("missing u_r5")?,
            gl.get_uniform_location(sphere_program, "u_r6").ok_or("missing u_r6")?,
            gl.get_uniform_location(sphere_program, "u_r7").ok_or("missing u_r7")?,
            gl.get_uniform_location(sphere_program, "u_r8").ok_or("missing u_r8")?,
            gl.get_uniform_location(sphere_program, "u_r9").ok_or("missing u_r9")?,
            gl.get_uniform_location(sphere_program, "u_r10").ok_or("missing u_r10")?,
            gl.get_uniform_location(sphere_program, "u_r11").ok_or("missing u_r11")?,
        ];
        Ok(GlPrograms {
            l_pos: gl.get_attrib_location(line_program, "a_world").ok_or("missing a_world")?,
            l_col: gl.get_attrib_location(line_program, "a_color").ok_or("missing a_color")?,
            p_pos: gl.get_attrib_location(point_program, "a_world").ok_or("missing a_world")?,
            p_col: gl.get_attrib_location(point_program, "a_color").ok_or("missing a_color")?,
            p_size: gl.get_attrib_location(point_program, "a_size").ok_or("missing a_size")?,
            s_ring: gl.get_attrib_location(sphere_program, "a_ring").ok_or("missing a_ring")?,
            s_angle: gl.get_attrib_location(sphere_program, "a_angle").ok_or("missing a_angle")?,
            l_mvp: gl
                .get_uniform_location(line_program, "u_mvp")
                .ok_or("missing u_mvp in line program")?,
            p_mvp: gl
                .get_uniform_location(point_program, "u_mvp")
                .ok_or("missing u_mvp in point program")?,
            p_pixel_ratio: gl
                .get_uniform_location(point_program, "u_pixel_ratio")
                .ok_or("missing u_pixel_ratio")?,
            p_zoom_scale: gl
                .get_uniform_location(point_program, "u_zoom_scale")
                .ok_or("missing u_zoom_scale")?,
            s_mvp: gl
                .get_uniform_location(sphere_program, "u_mvp")
                .ok_or("missing u_mvp in sphere program")?,
            s_target: gl
                .get_uniform_location(sphere_program, "u_target")
                .ok_or("missing u_target")?,
            s_distance: gl
                .get_uniform_location(sphere_program, "u_distance")
                .ok_or("missing u_distance")?,
            s_theta: gl
                .get_uniform_location(sphere_program, "u_theta")
                .ok_or("missing u_theta")?,
            s_phi: gl
                .get_uniform_location(sphere_program, "u_phi")
                .ok_or("missing u_phi")?,
            s_ring_count: gl
                .get_uniform_location(sphere_program, "u_ring_count")
                .ok_or("missing u_ring_count")?,
            s_radii,
            line_program,
            point_program,
            sphere_program,
        })
    }
}

fn create_program(gl: &glow::Context, vs: &str, fs: &str) -> Result<glow::Program, String> {
    unsafe {
        let vert = compile_shader(gl, glow::VERTEX_SHADER, vs)?;
        let frag = compile_shader(gl, glow::FRAGMENT_SHADER, fs)?;
        let prog = gl.create_program().map_err(|_| "create_program failed")?;
        gl.attach_shader(prog, vert);
        gl.attach_shader(prog, frag);
        gl.link_program(prog);
        gl.delete_shader(vert);
        gl.delete_shader(frag);
        if !gl.get_program_link_status(prog) {
            let log = gl.get_program_info_log(prog);
            gl.delete_program(prog);
            return Err(format!("link failed: {log}"));
        }
        Ok(prog)
    }
}

fn compile_shader(gl: &glow::Context, kind: u32, src: &str) -> Result<glow::Shader, String> {
    unsafe {
        let shader = gl.create_shader(kind).map_err(|_| "create_shader failed")?;
        gl.shader_source(shader, src);
        gl.compile_shader(shader);
        if !gl.get_shader_compile_status(shader) {
            let log = gl.get_shader_info_log(shader);
            gl.delete_shader(shader);
            return Err(format!("compile failed: {log}"));
        }
        Ok(shader)
    }
}

pub fn upload_buffer(gl: &glow::Context, buf: glow::Buffer, data: &[f32], usage: u32) {
    unsafe {
        gl.bind_buffer(glow::ARRAY_BUFFER, Some(buf));
        gl.buffer_data_u8_slice(glow::ARRAY_BUFFER, f32_slice_as_u8(data), usage);
    }
}

fn f32_slice_as_u8(data: &[f32]) -> &[u8] {
    unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, data.len() * 4) }
}

pub fn draw_lines(gl: &glow::Context, programs: &GlPrograms, buf: glow::Buffer, mvp: &[f32; 16], count: i32) {
    if count <= 0 {
        return;
    }
    unsafe {
        gl.use_program(Some(programs.line_program));
        gl.uniform_matrix_4_f32_slice(Some(&programs.l_mvp), false, mvp);
        gl.bind_buffer(glow::ARRAY_BUFFER, Some(buf));
        gl.enable_vertex_attrib_array(programs.l_pos);
        gl.enable_vertex_attrib_array(programs.l_col);
        gl.vertex_attrib_pointer_f32(programs.l_pos, 3, glow::FLOAT, false, 28, 0);
        gl.vertex_attrib_pointer_f32(programs.l_col, 4, glow::FLOAT, false, 28, 12);
        gl.draw_arrays(glow::LINES, 0, count);
    }
}

pub fn draw_glow_lines(gl: &glow::Context, programs: &GlPrograms, buf: glow::Buffer, mvp: &[f32; 16], count: i32) {
    unsafe {
        gl.blend_func(glow::SRC_ALPHA, glow::ONE);
    }
    draw_lines(gl, programs, buf, mvp, count);
    unsafe {
        gl.blend_func(glow::SRC_ALPHA, glow::ONE_MINUS_SRC_ALPHA);
    }
}

pub fn draw_triangle_fan(
    gl: &glow::Context,
    programs: &GlPrograms,
    buf: glow::Buffer,
    mvp: &[f32; 16],
    first: i32,
    count: i32,
) {
    if count <= 0 {
        return;
    }
    unsafe {
        gl.use_program(Some(programs.line_program));
        gl.uniform_matrix_4_f32_slice(Some(&programs.l_mvp), false, mvp);
        gl.bind_buffer(glow::ARRAY_BUFFER, Some(buf));
        gl.enable_vertex_attrib_array(programs.l_pos);
        gl.enable_vertex_attrib_array(programs.l_col);
        gl.vertex_attrib_pointer_f32(programs.l_pos, 3, glow::FLOAT, false, 28, 0);
        gl.vertex_attrib_pointer_f32(programs.l_col, 4, glow::FLOAT, false, 28, 12);
        gl.draw_arrays(glow::TRIANGLE_FAN, first, count);
    }
}

pub fn draw_glow_triangles(
    gl: &glow::Context,
    programs: &GlPrograms,
    buf: glow::Buffer,
    mvp: &[f32; 16],
    count: i32,
) {
    if count <= 0 {
        return;
    }
    unsafe {
        gl.blend_func(glow::SRC_ALPHA, glow::ONE);
    }
    draw_triangles(gl, programs, buf, mvp, count);
    unsafe {
        gl.blend_func(glow::SRC_ALPHA, glow::ONE_MINUS_SRC_ALPHA);
    }
}

fn draw_triangles(
    gl: &glow::Context,
    programs: &GlPrograms,
    buf: glow::Buffer,
    mvp: &[f32; 16],
    count: i32,
) {
    unsafe {
        gl.use_program(Some(programs.line_program));
        gl.uniform_matrix_4_f32_slice(Some(&programs.l_mvp), false, mvp);
        gl.bind_buffer(glow::ARRAY_BUFFER, Some(buf));
        gl.enable_vertex_attrib_array(programs.l_pos);
        gl.enable_vertex_attrib_array(programs.l_col);
        gl.vertex_attrib_pointer_f32(programs.l_pos, 3, glow::FLOAT, false, 28, 0);
        gl.vertex_attrib_pointer_f32(programs.l_col, 4, glow::FLOAT, false, 28, 12);
        gl.draw_arrays(glow::TRIANGLES, 0, count);
    }
}

pub fn draw_glow_triangle_fan(
    gl: &glow::Context,
    programs: &GlPrograms,
    buf: glow::Buffer,
    mvp: &[f32; 16],
    first: i32,
    count: i32,
) {
    unsafe {
        gl.blend_func(glow::SRC_ALPHA, glow::ONE);
    }
    draw_triangle_fan(gl, programs, buf, mvp, first, count);
    unsafe {
        gl.blend_func(glow::SRC_ALPHA, glow::ONE_MINUS_SRC_ALPHA);
    }
}

pub fn draw_points(
    gl: &glow::Context,
    programs: &GlPrograms,
    buf: glow::Buffer,
    mvp: &[f32; 16],
    pixel_ratio: f32,
    zoom_scale: f32,
    count: i32,
) {
    if count <= 0 {
        return;
    }
    unsafe {
        gl.use_program(Some(programs.point_program));
        gl.uniform_matrix_4_f32_slice(Some(&programs.p_mvp), false, mvp);
        gl.uniform_1_f32(Some(&programs.p_pixel_ratio), pixel_ratio);
        gl.uniform_1_f32(Some(&programs.p_zoom_scale), zoom_scale);
        gl.bind_buffer(glow::ARRAY_BUFFER, Some(buf));
        gl.enable_vertex_attrib_array(programs.p_pos);
        gl.enable_vertex_attrib_array(programs.p_col);
        gl.enable_vertex_attrib_array(programs.p_size);
        gl.vertex_attrib_pointer_f32(programs.p_pos, 3, glow::FLOAT, false, 32, 0);
        gl.vertex_attrib_pointer_f32(programs.p_col, 4, glow::FLOAT, false, 32, 12);
        gl.vertex_attrib_pointer_f32(programs.p_size, 1, glow::FLOAT, false, 32, 28);
        gl.draw_arrays(glow::POINTS, 0, count);
    }
}

pub fn draw_sphere_lines(
    gl: &glow::Context,
    programs: &GlPrograms,
    buf: glow::Buffer,
    mvp: &[f32; 16],
    target: [f32; 3],
    distance: f32,
    theta: f32,
    phi: f32,
    radii: &[f32],
    count: i32,
) {
    if count <= 0 || radii.is_empty() {
        return;
    }
    unsafe {
        gl.use_program(Some(programs.sphere_program));
        gl.uniform_matrix_4_f32_slice(Some(&programs.s_mvp), false, mvp);
        gl.uniform_3_f32(Some(&programs.s_target), target[0], target[1], target[2]);
        gl.uniform_1_f32(Some(&programs.s_distance), distance);
        gl.uniform_1_f32(Some(&programs.s_theta), theta);
        gl.uniform_1_f32(Some(&programs.s_phi), phi);
        gl.uniform_1_i32(Some(&programs.s_ring_count), radii.len() as i32);
        for (index, location) in programs.s_radii.iter().enumerate() {
            let radius = radii.get(index).copied().unwrap_or(0.0);
            gl.uniform_1_f32(Some(location), radius);
        }
        gl.bind_buffer(glow::ARRAY_BUFFER, Some(buf));
        gl.enable_vertex_attrib_array(programs.s_ring);
        gl.enable_vertex_attrib_array(programs.s_angle);
        gl.vertex_attrib_pointer_f32(programs.s_ring, 1, glow::FLOAT, false, 8, 0);
        gl.vertex_attrib_pointer_f32(programs.s_angle, 1, glow::FLOAT, false, 8, 4);
        gl.draw_arrays(glow::LINES, 0, count);
    }
}

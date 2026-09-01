/// Column-major 4x4 matrix for WebGL (indices: col*4 + row).
pub type Mat4 = [f32; 16];

pub fn look_at(ex: f32, ey: f32, ez: f32, tx: f32, ty: f32, tz: f32) -> Mat4 {
    let mut zx = ex - tx;
    let mut zy = ey - ty;
    let mut zz = ez - tz;
    let zl = (zx * zx + zy * zy + zz * zz).sqrt().max(1.0);
    zx /= zl;
    zy /= zl;
    zz /= zl;

    let mut xx = -zz;
    let xy = 0.0_f32;
    let mut xz = zx;
    let xl = (xx * xx + xy * xy + xz * xz).sqrt();
    if xl < 0.001 {
        xx = 1.0;
        xz = 0.0;
    } else {
        xx /= xl;
        xz /= xl;
    }

    let yx = zy * xz - zz * xy;
    let yy = zz * xx - zx * xz;
    let yz = zx * xy - zy * xx;

    [
        xx,
        yx,
        zx,
        0.0,
        xy,
        yy,
        zy,
        0.0,
        xz,
        yz,
        zz,
        0.0,
        -(xx * ex + xy * ey + xz * ez),
        -(yx * ex + yy * ey + yz * ez),
        -(zx * ex + zy * ey + zz * ez),
        1.0,
    ]
}

pub fn perspective(fov_y: f32, aspect: f32, near: f32, far: f32) -> Mat4 {
    let f = 1.0 / (fov_y / 2.0).tan();
    let nf = 1.0 / (near - far);
    [
        f / aspect,
        0.0,
        0.0,
        0.0,
        0.0,
        f,
        0.0,
        0.0,
        0.0,
        0.0,
        (near + far) * nf,
        -1.0,
        0.0,
        0.0,
        2.0 * near * far * nf,
        0.0,
    ]
}

pub fn multiply_mat4(a: &Mat4, b: &Mat4) -> Mat4 {
    let mut out = [0.0_f32; 16];
    for col in 0..4 {
        for row in 0..4 {
            let mut s = 0.0_f32;
            for k in 0..4 {
                s += a[k * 4 + row] * b[col * 4 + k];
            }
            out[col * 4 + row] = s;
        }
    }
    out
}

pub fn build_mvp(
    theta: f32,
    phi: f32,
    distance: f32,
    tx: f32,
    ty: f32,
    tz: f32,
    aspect: f32,
) -> Mat4 {
    let cos_p = phi.cos();
    let sin_p = phi.sin();
    let cos_t = theta.cos();
    let sin_t = theta.sin();
    let eye_x = tx + distance * cos_p * sin_t;
    let eye_y = ty + distance * sin_p;
    let eye_z = tz + distance * cos_p * cos_t;
    let proj = perspective(
        std::f32::consts::FRAC_PI_3,
        aspect,
        distance * 0.001,
        distance * 12.0 + 50.0,
    );
    let view = look_at(eye_x, eye_y, eye_z, tx, ty, tz);
    multiply_mat4(&proj, &view)
}

pub fn project_point(
    mvp: &Mat4,
    x: f32,
    y: f32,
    z: f32,
    css_w: f32,
    css_h: f32,
) -> Option<(f32, f32)> {
    let cx = mvp[0] * x + mvp[4] * y + mvp[8] * z + mvp[12];
    let cy = mvp[1] * x + mvp[5] * y + mvp[9] * z + mvp[13];
    let cw = mvp[3] * x + mvp[7] * y + mvp[11] * z + mvp[15];
    if cw <= 0.0 {
        return None;
    }
    Some(((cx / cw + 1.0) * 0.5 * css_w, (1.0 - cy / cw) * 0.5 * css_h))
}

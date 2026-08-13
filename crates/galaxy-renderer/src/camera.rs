#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Camera {
    pub theta: f32,
    pub phi: f32,
    pub distance: f32,
    pub target_x: f32,
    pub target_y: f32,
    pub target_z: f32,
}

impl Default for Camera {
    fn default() -> Self {
        Self {
            theta: 0.4,
            phi: 0.5,
            distance: 20.0,
            target_x: 0.0,
            target_y: 0.0,
            target_z: 0.0,
        }
    }
}

pub const ROTATE_SPEED: f32 = 0.007;
pub const PAN_SPEED: f32 = 0.0018;
pub const ZOOM_SPEED: f32 = 0.0005;
pub const MIN_DIST: f32 = 0.5;
pub const MAX_DIST: f32 = 600.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DragMode {
    Rotate,
    Pan,
}

#[derive(Clone, Copy, Debug)]
pub struct DragState {
    pub start_x: f32,
    pub start_y: f32,
    pub start_theta: f32,
    pub start_phi: f32,
    pub start_tx: f32,
    pub start_ty: f32,
    pub start_tz: f32,
    pub mode: DragMode,
    pub moved: bool,
}

impl Camera {
    pub fn camera_vectors(&self) -> (f32, f32, f32, f32, f32, f32) {
        (
            self.theta.cos(),
            0.0,
            -self.theta.sin(),
            -self.theta.sin() * self.phi.sin(),
            self.phi.cos(),
            -self.theta.cos() * self.phi.sin(),
        )
    }

    pub fn apply_drag(&mut self, drag: &DragState, x: f32, y: f32) {
        let dx = x - drag.start_x;
        let dy = y - drag.start_y;
        match drag.mode {
            DragMode::Rotate => {
                self.theta = drag.start_theta + dx * ROTATE_SPEED;
                self.phi = (drag.start_phi - dy * ROTATE_SPEED)
                    .clamp(-std::f32::consts::FRAC_PI_2 + 0.02, std::f32::consts::FRAC_PI_2 - 0.02);
            }
            DragMode::Pan => {
                let scale = self.distance * PAN_SPEED;
                let (rx, ry, rz, ux, uy, uz) = self.camera_vectors();
                self.target_x = drag.start_tx + rx * dx * scale - ux * dy * scale;
                self.target_y = drag.start_ty + ry * dx * scale - uy * dy * scale;
                self.target_z = drag.start_tz + rz * dx * scale - uz * dy * scale;
            }
        }
    }

    pub fn zoom(&mut self, delta_y: f32) {
        self.distance = (self.distance * (delta_y * ZOOM_SPEED).exp()).clamp(MIN_DIST, MAX_DIST);
    }
}

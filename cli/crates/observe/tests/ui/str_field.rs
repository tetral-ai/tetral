fn main() {
    let path: &str = "/home/someone/project";
    observe::info!("cold_start.phase_started", cold_start.phase = path);
}

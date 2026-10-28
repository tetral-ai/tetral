fn main() {
    let path = String::from("/home/someone/project");
    observe::info!("cold_start.phase_started", cold_start.phase = path);
}

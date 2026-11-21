//! `sim <seed>`: runs the simulation of the engine from one seed, one JSON
//! line per step and a verdict (`truefit help sim`).

fn main() {
    let seed = std::env::args().nth(1).unwrap_or_else(|| "0".into());
    factum_conformance::sim::run(&seed);
}

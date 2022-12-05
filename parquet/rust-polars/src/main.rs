use polars::prelude::*;
use std::{thread, time::Duration, env};

fn main(){
    let args: Vec<String> = env::args().collect();
    let cache_file = args[1].as_str();
    let mut file = std::fs::File::open(cache_file).unwrap();
    let df = ParquetReader::new(&mut file).finish().unwrap();
    println!("{:?}",df.shape());
    let datas =df.get(1).unwrap();
    println!("{:?}",     datas[0]);
    println!("{:?}",     datas[10]);
    thread::sleep(Duration::from_secs(60));

}

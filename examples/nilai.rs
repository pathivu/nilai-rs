use nilai::builder;
fn main(){
    let nilai_builder = builder::NilaiBuilder::new("127.0.0.1:5001".parse().unwrap());
    nilai_builder.execute().unwrap();
}
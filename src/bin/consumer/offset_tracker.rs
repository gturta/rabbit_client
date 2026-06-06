use rabbit_client::error::AppError;

pub struct OffsetTracker{
    name: String,
    db: sled::Db,
}

impl OffsetTracker{
    pub fn new(name: String, path: &String) -> Result<Self, AppError> {
        let db = sled::open(path)?;
        Ok(OffsetTracker{ name, db })
    }
    pub fn write(&self, offset: u64){
        self.db.insert(&self.name, &offset.to_be_bytes()).expect("failed writing offset into sled db");
        self.db.flush().expect("failed flush operation for sled db");
    }
    pub fn read(&self) -> Result<Option<u64>, AppError>{
        let maybe_ivec = self.db.get(&self.name)?;
        let maybe_u64 = maybe_ivec.map(|ivec| {
            let u64_as_array: [u8;8] = ivec.as_ref().try_into().expect("Invalid offset format in sled db");
            u64::from_be_bytes(u64_as_array)
        });
        Ok(maybe_u64)
    }
}

use rabbit_client::error::AppError;
use tokio::sync::mpsc;
use tokio::time;

pub struct OffsetTracker{
    name: String,
    db: sled::Db,
}

impl OffsetTracker{
    pub fn new(name: String, path: &String) -> Result<Self, AppError> {
        let db = sled::open(path)?;
        Ok(OffsetTracker{ name, db })
    }
    
    pub fn read(&self) -> Result<Option<u64>, AppError>{
        let maybe_ivec = self.db.get(&self.name)?;
        let maybe_u64 = maybe_ivec.map(|ivec| {
            let u64_as_array: [u8;8] = ivec.as_ref().try_into().expect("Invalid offset format in sled db");
            u64::from_be_bytes(u64_as_array)
        });
        Ok(maybe_u64)
    }

    pub fn write(&self, offset: u64){
        self.db.insert(&self.name, &offset.to_be_bytes()).expect("failed writing offset into sled db");
        self.db.flush().expect("failed flush operation for sled db");
    }

    // ideea e ca daca vreau sa pot scrie async offsetul curent atunci trebuie sa o fac pe un worker
    // separat
    // asa ca transform clasa initiala intr-una care muta conexiunea pe un worker separat care
    // primeste mesaje de scriere in db
    pub fn into_async(self) -> AsyncOffsetTracker {
        //communication channel
        let (tx, rx) = mpsc::channel(10);
        //start async worker
        let handle = tokio::spawn(AsyncOffsetTracker::process_task(rx, self.db, self.name));
        AsyncOffsetTracker { sender: tx, handle }
    }
}

pub struct AsyncOffsetTracker {
    sender: mpsc::Sender<u64>,
    handle: tokio::task::JoinHandle<()>,
}
impl AsyncOffsetTracker {
    pub async fn process_task(mut rx: mpsc::Receiver<u64>, db: sled::Db, name: String) {
        //let's do a flush every 200ms; this is the start time
        let mut last_flush = time::Instant::now();
       
        //offset persistence worker main cycle
        while let Some(value) = rx.recv().await {
            db.insert(&name, &value.to_be_bytes()).expect("failed writing offset into sled db");
            //let's do a flush every 200ms
            if last_flush + time::Duration::from_millis(200) < time::Instant::now() {
                db.flush_async().await.expect("failed flush operation for sled db");
                last_flush = time::Instant::now();
            }
        }
    }

    pub async fn write(&self, value: u64) {
        self.sender.send(value).await.expect("failed to put write message on channel");
    }

    pub async fn close(self) -> Result<(), tokio::task::JoinError> {
        self.handle.await?;
        Ok(())
    }
}

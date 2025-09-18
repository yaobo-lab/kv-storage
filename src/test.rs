#[cfg(test)]
#[allow(unused_imports)]
mod tests {
    use super::super::sled_storage::{SledStorageDB, SledStorageList, SledStorageMap};
    use super::super::*;
    use std::borrow::Cow;
    use std::time::Duration;
    use tokio::time::sleep;
    async fn get_db(name: &str) -> StorageDB {
        let cfg = Config {
            path: format!("./db/list/{}", name),
            ..Default::default()
        };
        let db = init_db(&cfg).await.unwrap();
        db
    }

    #[tokio::main]
    #[test]
    async fn test_counter() {
        let db = get_db("incr").await;

        db.remove("incr1").await.unwrap();
        db.remove("incr2").await.unwrap();
        db.remove("incr3").await.unwrap();

        db.counter_incr("incr1", 3).await.unwrap();
        db.counter_incr("incr2", -3).await.unwrap();
        db.counter_incr("incr3", 10).await.unwrap();

        assert_eq!(db.counter_get("incr1").await.unwrap(), Some(3));
        assert_eq!(db.counter_get("incr2").await.unwrap(), Some(-3));
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(10));

        db.counter_decr("incr3", 2).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(8));

        db.counter_decr("incr3", -3).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(11));

        db.counter_set("incr3", 100).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(100));

        db.counter_incr("incr3", 10).await.unwrap();
        assert_eq!(db.counter_get("incr3").await.unwrap(), Some(110));

        assert_eq!(db.counter_get("incr4").await.unwrap(), None);
    }

    #[tokio::main]
    #[test]
    async fn test_db_batch() {
        let db = get_db("db_batch_insert").await;

        let mut key_vals = Vec::new();
        for i in 0..100 {
            key_vals.push((format!("key_{}", i).as_bytes().to_vec(), i));
        }

        db.batch_insert(key_vals).await.unwrap();

        assert_eq!(db.get("key_99").await.unwrap(), Some(99));
        assert_eq!(db.get::<_, usize>("key_100").await.unwrap(), None);

        let mut keys = Vec::new();
        for i in 0..50 {
            keys.push(format!("key_{}", i).as_bytes().to_vec());
        }
        db.batch_remove(keys).await.unwrap();

        assert_eq!(db.get::<_, usize>("key_0").await.unwrap(), None);
        assert_eq!(db.get::<_, usize>("key_49").await.unwrap(), None);
        assert_eq!(db.get("key_50").await.unwrap(), Some(50));

        let mut keys = Vec::new();
        for i in 50..100 {
            keys.push(format!("key_{}", i).as_bytes().to_vec());
        }
        db.batch_remove(keys).await.unwrap();
    }

    #[tokio::main]
    #[test]
    async fn test_session_iter() {
        let mut db = get_db("session").await;
        let now = std::time::Instant::now();
        let mut iter = db.map_iter().await.unwrap();
        let mut count = 0;
        while let Some(m) = iter.next().await {
            let _m = m.unwrap();
            //println!("map name: {:?}", String::from_utf8_lossy(m.name()));
            count += 1;
        }
        println!("count: {}, cost time: {:?}", count, now.elapsed());
    }

    #[tokio::main]
    #[test]
    async fn test_db_size() {
        let mut db = get_db("db_size").await;
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            db.remove(item).await.unwrap();
        }
        println!("test_db_size db_size: {:?}", db.db_size().await);
        db.insert("k1", &1).await.unwrap();
        db.insert("k2", &2).await.unwrap();
        db.insert("k3", &3).await.unwrap();
        println!("test_db_size db_size: {:?}", db.db_size().await);
        let m = db.map("map1", None).await.unwrap();
        m.insert("mk1", &1).await.unwrap();
        m.insert("mk2", &2).await.unwrap();
        println!("test_db_size db_size: {:?}", db.db_size().await);

        db.batch_insert(vec![
            (Vec::from("batch/len/1"), 11),
            (Vec::from("batch/len/2"), 22),
            (Vec::from("batch/len/3"), 33),
        ])
        .await
        .unwrap();
        println!("test_db_size db_size: {:?}", db.db_size().await);
    }

    async fn collect(mut iter: Box<dyn AsyncIterator<Item = Result<Key>> + Send + '_>) -> Vec<Key> {
        let mut data = Vec::new();
        while let Some(key) = iter.next().await {
            data.push(key.unwrap())
        }
        data
    }

    #[tokio::main]
    // #[test]
    async fn test_len() {
        let mut db = get_db("test_len").await;
        println!("a test_len len: {:?}", db.len().await);
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            println!(
                "test_len remove item: {:?}",
                String::from_utf8_lossy(item.as_slice())
            );
            db.remove(item).await.unwrap();
        }
        println!("b test_len len: {:?}", db.len().await);
        db.insert("foo/len/1", &1).await.unwrap();
        db.insert("foo/len/2", &2).await.unwrap();
        db.insert("foo/len/3", &3).await.unwrap();
        db.insert("foo/len/4", &4).await.unwrap();

        db.expire_at("foo/len/3", timestamp_millis() + 1 * 1000)
            .await
            .unwrap();
        db.expire("foo/len/4", 1000 * 2).await.unwrap();
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 4);

        sleep(Duration::from_millis(1100)).await;
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 3);

        sleep(Duration::from_millis(1100)).await;
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 2);

        db.remove("foo/len/1").await.unwrap();
        println!("test_len len: {:?}", db.len().await);
        assert_eq!(db.len().await.unwrap(), 1);

        db.batch_insert(vec![
            (Vec::from("batch/len/1"), 11),
            (Vec::from("batch/len/2"), 22),
            (Vec::from("batch/len/3"), 33),
        ])
        .await
        .unwrap();
        assert_eq!(db.len().await.unwrap(), 4);

        db.batch_remove(vec![Vec::from("batch/len/1"), Vec::from("batch/len/2")])
            .await
            .unwrap();

        assert_eq!(db.len().await.unwrap(), 2);
        println!("test_len len: {:?}", db.len().await);
    }
}

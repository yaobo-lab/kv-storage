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
            path: format!("./db/map/{}", name),
            ..Default::default()
        };
        let db = init_db(&cfg).await.unwrap();
        db
    }

    #[tokio::main]
    #[test]
    async fn test_map_insert() {
        let db = get_db("map_insert").await;

        let map001 = db.map("001", None).await.unwrap();
        map001.clear().await.unwrap();

        map001.insert("key_1", &1).await.unwrap();
        map001.insert("key_2", &2).await.unwrap();
        println!("map001 len:{} is 2", map001.len().await.unwrap());

        let val = map001.get::<_, i32>("key_1").await.unwrap();
        println!("get key_1 val: {:?}", val);

        map001.remove::<_>("key_1").await.unwrap();
        let val = map001.get::<_, i32>("key_1").await.unwrap();
        println!("remove key_1 val: {:?}", val);

        println!("test_map_insert len: {:?}", map001.len().await.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_map_contains_key() {
        let db = get_db("map_contains_key").await;

        let map001 = db.map("m001", None).await.unwrap();
        map001.clear().await.unwrap();

        println!("map001 len:{}", map001.len().await.unwrap());

        map001.insert("k001", &"val_001").await.unwrap();
        println!(
            "contains_key k001:{} ",
            map001.contains_key("k001").await.unwrap()
        );

        map001.remove::<_>("k001").await.unwrap();
        println!(
            "contains_key k001:{} ",
            map001.contains_key("k001").await.unwrap()
        );
    }

    #[tokio::main]
    #[test]
    async fn test_map_expire() {
        let db = get_db("map_expire").await;

        let map1 = db.map("map1", Some(1000)).await.unwrap();
        println!("1.map1 ttl: {:?}", map1.ttl().await.unwrap());

        map1.insert("k1", &1).await.unwrap();
        map1.insert("k2", &2).await.unwrap();

        println!("2.map1 ttl: {:?}", map1.ttl().await.unwrap());
        println!("2.map1.is_empty: {:?}", map1.is_empty().await.unwrap());
        println!("2.map1.len: {:?} is 2?", map1.len().await.unwrap());

        sleep(Duration::from_millis(1200)).await;
        println!("3.map1 ttl: {:?}", map1.ttl().await.unwrap());
        println!("3.map1.len: {:?} is 0?", map1.len().await.unwrap());
        println!("3.map1.is_empty: {:?}", map1.is_empty().await.unwrap());
    }

    #[tokio::main]
    #[test]
    async fn test_map_expire2() {
        let db = get_db("map_expire").await;
        let mut db1 = db.clone();
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_millis(10000)).await;
                let mut iter = db1.map_iter().await.unwrap();
                let limit = 10;
                let mut c = 0;
                while let Some(map) = iter.next().await {
                    let map = map.unwrap();
                    println!(
                        "map.is_empty(): {:?}, now: {:?}",
                        map.is_empty().await.unwrap(),
                        timestamp_millis()
                    );
                    c += 1;
                    if c > limit {
                        break;
                    }
                }
            }
        });

        for x in 0..500 {
            let db = db.clone();
            tokio::spawn(async move {
                for i in 0..10_000 {
                    let map = match db
                        .map(format!("map_{}_{}", x, i), Some(1000 * 60))
                        //.map_expire(format!("map_{}_{}", x, i), None)
                        .await
                    {
                        Ok(map) => map,
                        Err(e) => {
                            println!("map_expire {:?}", e);
                            continue;
                        }
                    };
                    if let Err(e) = map.insert(format!("k1_{}", i), &i).await {
                        println!("insert {:?}", e);
                    }
                    sleep(Duration::from_millis(0)).await;
                    if let Err(e) = map.insert(format!("k2_{}", i), &i).await {
                        println!("insert {:?}", e);
                    }
                    sleep(Duration::from_millis(0)).await;
                    if let Err(e) = map.insert(format!("k3_{}", i), &i).await {
                        println!("insert {:?}", e);
                    }
                    sleep(Duration::from_millis(0)).await;
                }
                println!("********************* end {:?}", x);
            });
        }

        sleep(Duration::from_secs(100000)).await;
    }

    #[tokio::main]
    #[test]
    async fn test_map_iter() {
        let mut db = get_db("map_iter").await;
        let mut map_iter = db.map_iter().await.unwrap();
        while let Some(map) = map_iter.next().await {
            let map = map.unwrap();
            map.clear().await.unwrap();
        }
        drop(map_iter);

        let mut map1 = db.map("map-1", None).await.unwrap();
        for i in 0..10 {
            map1.insert(format!("map-val-{}", i), &i).await.unwrap();
        }
        let map_val_5 = map1.get::<_, i32>("map-val-5").await.unwrap();
        println!("get val map-val-5: {:?}", map_val_5);

        //遍历map
        let mut iter = map1.iter::<i32>().await.unwrap();
        while let Some(v) = iter.next().await {
            let (key, val) = v.unwrap();
            println!("key: {:?}, val: {:?}", String::from_utf8_lossy(&key), val);
        }
    }

    #[tokio::main]
    #[test]
    async fn test_map_iter_all() {
        let mut db = get_db("map_iter_all").await;
        let mut map_iter = db.map_iter().await.unwrap();
        while let Some(map) = map_iter.next().await {
            let map = map.unwrap();
            map.clear().await.unwrap();
        }
        drop(map_iter);

        let max = 10;
        for i in 0..max {
            let map1 = db.map(format!("map-{}", i), None).await.unwrap();
            for k in 0..3 {
                map1.insert(format!("map-{}-{}", i, k), &k).await.unwrap();
            }
        }

        let mut map_iter = db.map_iter().await.unwrap();
        while let Some(map) = map_iter.next().await {
            let mut map = map.unwrap();

            println!("map=====>: {:?}", String::from_utf8_lossy(map.name()));

            let mut iter = map.iter::<i32>().await.unwrap();
            while let Some(item) = iter.next().await {
                let (key, val) = item.unwrap();
                println!("key: {:?}, val: {:?}", String::from_utf8_lossy(&key), val);
            }
        }
    }
}

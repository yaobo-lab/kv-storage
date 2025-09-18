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
            path: format!("./db/test/{}", name),
            ..Default::default()
        };
        let db = init_db(&cfg).await.unwrap();
        db
    }
    #[tokio::main]
    #[test]
    async fn test_kv_insert() {
        let db = get_db("test_kv_insert").await;
        let db_key_1 = b"key_1";
        let db_key_2 = b"key_2";
        let db_val_1 = String::from("val_001");
        let db_val_2 = String::from("val_002");
        db.insert::<_, String>(db_key_1, &db_val_1).await.unwrap();
        db.insert::<_, String>(db_key_2, &db_val_2).await.unwrap();
        println!(
            "contains_key key_1:{} ",
            db.contains_key(&db_key_1).await.unwrap(),
        );

        let val = db.get::<_, String>(db_key_1).await.unwrap();
        let val2 = db.get::<_, String>(db_key_2).await.unwrap();
        println!(
            "key_1: {},key_2: {}",
            val.unwrap_or("".to_string()),
            val2.unwrap_or("".to_string())
        );

        //移除
        db.remove(db_key_1).await.unwrap();
        let val = db.get::<_, String>(db_key_1).await.unwrap();

        println!("remove key_1 val: {}", val.unwrap_or("null".to_string()));
        println!(
            "contains_key key_1:{} ",
            db.contains_key(&db_key_1).await.unwrap(),
        );

        let k2 = db.contains_key(&db_key_2).await.unwrap();
        println!("key_2 contains_key:{}", k2);

        let val2 = db.get::<_, String>(db_key_2).await.unwrap();
        let k2 = db.contains_key(&db_key_2).await.unwrap();
        println!(
            "key_2: {},contains_key:{}",
            val2.unwrap_or("".to_string()),
            k2
        );
    }

    #[tokio::main]
    #[test]
    async fn test_expiration_cleaning() {
        let db = get_db("expiration_cleaning").await;
        for i in 0..3usize {
            let key = format!("k_{}", i);
            db.insert(key.as_bytes(), &format!("v_{}", (i * 10)))
                .await
                .unwrap();
            let _ = db.expire(key, 1500).await.unwrap();
        }

        println!(
            "1.db size: {} Len:{}",
            db.db_size().await.unwrap(),
            //Gets number of key-value pairs (if enabled)
            db.len().await.unwrap()
        );

        let map_db = db.map("m_1", None).await.unwrap();
        map_db.insert("m_k_1", &1).await.unwrap();
        map_db.insert("m_k_2", &2).await.unwrap();
        let _ = map_db.expire(1500).await.unwrap();

        println!(
            "2.db size: {} Len:{}",
            db.db_size().await.unwrap(),
            db.len().await.unwrap()
        );

        let list_db = db.list("l_1", None).await.unwrap();
        list_db.clear().await.unwrap();
        list_db.push(&11).await.unwrap();
        list_db.push(&22).await.unwrap();
        let _ = list_db.expire(1500).await.unwrap();

        println!(
            "3.db size: {} Len:{}",
            db.db_size().await.unwrap(),
            db.len().await.unwrap()
        );

        tokio::time::sleep(Duration::from_millis(1700)).await;

        //kv值
        let k_0_val = db.get::<_, String>("k_0").await.unwrap();
        println!("kv值 k_0_val: {:?}", k_0_val);

        //map值
        let m_k_2 = map_db.get::<_, i32>("m_k_2").await.unwrap();
        println!("map值 m_k_2: {:?}", m_k_2);

        //list值
        let l_all = list_db.all::<i32>().await.unwrap();
        println!("list值 all: {:?}", l_all);

        println!(
            "4.db size: {} Len:{}",
            db.db_size().await.unwrap(),
            //Gets number of key-value pairs (if enabled)
            db.len().await.unwrap()
        );

        tokio::time::sleep(Duration::from_secs(25)).await;

        println!(
            "5.db size: {} Len:{}",
            db.db_size().await.unwrap(),
            //Gets number of key-value pairs (if enabled)
            db.len().await.unwrap()
        );
    }

    #[tokio::main]
    #[test]
    async fn test_sled_cleanup() {
        let db = get_db("cleanup").await;
        let max = 3000;

        for i in 0..max {
            let map = db.map(format!("map_{}", i), None).await.unwrap();
            map.insert("k_1", &1).await.unwrap();
            map.insert("k_2", &2).await.unwrap();
            map.expire(100).await.unwrap();
        }

        for i in 0..max {
            let list = db.list(format!("list_{}", i), None).await.unwrap();
            list.push(&1).await.unwrap();
            list.push(&2).await.unwrap();
            list.expire(100).await.unwrap();
        }

        tokio::time::sleep(Duration::from_millis(120)).await;

        println!(
            "1. db_size: {:?}",
            db.db_size().await,
            // db.map_size(),
            // db.list_size()
        );
        tokio::time::sleep(Duration::from_secs(13)).await;

        //tokio::time::sleep(Duration::from_secs(3)).await;
        println!(
            "2. db_size: {:?}",
            db.db_size().await,
            // db.map_size(),
            // db.list_size()
        );
    }

    #[tokio::main]
    #[test]
    async fn test_iter() {
        let db = get_db("iter").await;
        let mut skv = db.map("iter_kv002", None).await.unwrap();
        skv.clear().await.unwrap();

        for i in 0..10 {
            skv.insert::<_, i32>(format!("key_{}", i), &i)
                .await
                .unwrap();
        }

        let mut vals = Vec::new();
        let mut iter = skv.iter::<i32>().await.unwrap();
        while let Some(item) = iter.next().await {
            vals.push(item.unwrap())
        }
        drop(iter);

        assert_eq!(
            vals,
            vec![
                (b"key_0".to_vec(), 0),
                (b"key_1".to_vec(), 1),
                (b"key_2".to_vec(), 2),
                (b"key_3".to_vec(), 3),
                (b"key_4".to_vec(), 4),
                (b"key_5".to_vec(), 5),
                (b"key_6".to_vec(), 6),
                (b"key_7".to_vec(), 7),
                (b"key_8".to_vec(), 8),
                (b"key_9".to_vec(), 9),
            ]
        );

        let mut keys = Vec::new();
        let mut key_iter = skv.key_iter().await.unwrap();
        while let Some(item) = key_iter.next().await {
            keys.push(String::from_utf8(item.unwrap()).unwrap())
        }
        drop(key_iter);

        assert_eq!(
            keys,
            vec![
                "key_0", "key_1", "key_2", "key_3", "key_4", "key_5", "key_6", "key_7", "key_8",
                "key_9"
            ]
        );

        for i in 0..5 {
            skv.insert::<_, i32>(format!("key2_{}", i), &i)
                .await
                .unwrap();
        }

        let mut vals = Vec::new();
        let mut prefix_iter = skv.prefix_iter::<_, i32>("key2_").await.unwrap();
        while let Some(item) = prefix_iter.next().await {
            vals.push(item.unwrap())
        }

        assert_eq!(
            vals,
            vec![
                (b"key2_0".to_vec(), 0),
                (b"key2_1".to_vec(), 1),
                (b"key2_2".to_vec(), 2),
                (b"key2_3".to_vec(), 3),
                (b"key2_4".to_vec(), 4)
            ]
        );
    }

    #[tokio::main]
    #[test]
    async fn test_stress() {
        let db = get_db("stress").await;
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            db.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        let k_9999_val = db.get::<_, usize>(9999usize.to_be_bytes()).await.unwrap();
        println!(
            "test_stress 9999: {:?}, cost time: {:?}",
            k_9999_val,
            now.elapsed()
        );
        assert_eq!(k_9999_val, Some(9999));

        let s_m_1 = db.map("s_m_1", None).await.unwrap();
        s_m_1.clear().await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            s_m_1.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        #[cfg(feature = "map_len")]
        assert_eq!(s_m_1.len().await.unwrap(), 10_000);
        let k_9999_val = s_m_1
            .get::<_, usize>(9999usize.to_be_bytes())
            .await
            .unwrap();
        println!(
            "test_stress s_m_1 9999: {:?}, cost time: {:?}",
            k_9999_val,
            now.elapsed()
        );
        assert_eq!(k_9999_val, Some(9999));

        let s_l_1 = db.list("s_l_1", None).await.unwrap();
        s_l_1.clear().await.unwrap();
        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            s_l_1.push(&i).await.unwrap();
        }
        println!("test_stress s_l_1: {:?}", s_l_1.len().await.unwrap());
        assert_eq!(s_l_1.len().await.unwrap(), 10_000);
        let l_9999_val = s_l_1.get_index::<usize>(9999).await.unwrap();
        println!(
            "test_stress s_l_1 9999: {:?}, cost time: {:?}",
            l_9999_val,
            now.elapsed()
        );
        assert_eq!(l_9999_val, Some(9999));

        tokio::time::sleep(Duration::from_secs(1)).await;

        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            let s_m = db.map(format!("s_m_{}", i), None).await.unwrap();
            s_m.insert(i.to_be_bytes(), &i).await.unwrap();
        }
        println!("test_stress s_m, cost time: {:?}", now.elapsed());

        let now = std::time::Instant::now();
        for i in 0..10_000usize {
            let s_l = db.list(format!("s_l_{}", i), None).await.unwrap();
            s_l.push(&i).await.unwrap();
        }
        println!("test_stress s_l, cost time: {:?}", now.elapsed());

        tokio::time::sleep(Duration::from_secs(3)).await;
        println!("$$$ test_stress db_size: {:?}", db.db_size().await,);
    }

    #[tokio::main]
    #[test]
    async fn test_batch() {
        let db = get_db("batch").await;

        let skv = db.map("batch_kv001", None).await.unwrap();

        let mut kvs = Vec::new();
        for i in 0..100 {
            kvs.push((format!("key_{}", i).as_bytes().to_vec(), i));
        }
        skv.batch_insert(kvs.clone()).await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(skv.len().await.unwrap(), 100);

        let mut ks = Vec::new();
        for i in 0..50 {
            ks.push(format!("key_{}", i).as_bytes().to_vec());
        }
        skv.batch_remove(ks).await.unwrap();
        #[cfg(feature = "map_len")]
        assert_eq!(skv.len().await.unwrap(), 50);
    }
}

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
    async fn test_insert_list() {
        let db = get_db("array").await;
        let array_a = db.list("array_a", None).await.unwrap();
        let array_b = db.list("array_b", None).await.unwrap();
        let mut array_c = db.list("array_c", None).await.unwrap();

        array_a.clear().await.unwrap();
        array_b.clear().await.unwrap();
        array_c.clear().await.unwrap();

        db.insert("key_001", &1).await.unwrap();
        db.insert("key_002", &2).await.unwrap();
        db.insert("key_003", &3).await.unwrap();

        for i in 0..5 {
            array_a.push(&i).await.unwrap();
        }
        println!("array_a len: {}", array_a.len().await.unwrap());
        let vals = array_a.all::<i32>().await.unwrap();
        println!("array_a vals: {:?}", vals);

        let val_1 = array_a.get_index::<i32>(1).await.unwrap();
        println!("get_index 1: {:?}", val_1);

        let val_0 = array_a.pop::<i32>().await.unwrap();
        println!("array_a pop: {:?}", val_0);
        println!("array_a len: {}", array_a.len().await.unwrap());

        let val_1 = array_a.pop::<i32>().await.unwrap();
        println!("array_a pop: {:?}", val_1);

        let vals = array_a.all::<i32>().await.unwrap();
        println!("array_a vals: {:?}", vals);

        println!("storage list push_limit============>");
        //只保留最后5个
        for i in 0..20 {
            array_a.push_limit(&i, 5, true).await.unwrap();
        }
        println!("array_a len: {} 5", array_a.len().await.unwrap());
        let vals = array_a.all::<i32>().await.unwrap();
        println!("array_a vals: {:?}", vals);

        println!("list push b、c ============>");
        for i in 0..4 {
            array_b.push(&i).await.unwrap();
        }
        for i in 0..3 {
            array_c.push(&i).await.unwrap();
        }

        // 遍历
        let mut iter = array_c.iter::<i32>().await.unwrap();
        while let Some(val) = iter.next().await {
            let val = val.unwrap();
            println!("1.array_c iter val: {}", val);
        }
        drop(iter);

        // 遍历
        let mut iter = array_c.iter::<i32>().await.unwrap();
        while let Some(val) = iter.next().await {
            let val = val.unwrap();
            println!("2.array_c iter val: {}", val);
        }
    }

    #[tokio::main]
    #[test]
    async fn test_list_iter() {
        let mut db = get_db("list_iter").await;
        let array_a = db.list("array_a", None).await.unwrap();
        let array_b = db.list("array_b", None).await.unwrap();
        for i in 0..5 {
            array_a.push(&i).await.unwrap();
        }
        for i in 0..5 {
            array_b.push(&i).await.unwrap();
        }

        let mut list_iter = db.list_iter().await.unwrap();
        while let Some(list) = list_iter.next().await {
            let mut list = list.unwrap();
            println!("list=====>: {:?}", String::from_utf8_lossy(list.name()));
            let mut iter = list.iter::<i32>().await.unwrap();
            while let Some(item) = iter.next().await {
                let val = item.unwrap();
                println!("val: {}", val);
            }
        }
    }

    #[tokio::main]
    #[test]
    async fn test_clear() {
        let mut db = get_db("test_clear").await;
        let mut list_iter = db.list_iter().await.unwrap();
        while let Some(list) = list_iter.next().await {
            let list = list.unwrap();
            list.clear().await.unwrap();
        }
    }

    async fn collect(mut iter: Box<dyn AsyncIterator<Item = Result<Key>> + Send + '_>) -> Vec<Key> {
        let mut data = Vec::new();
        while let Some(key) = iter.next().await {
            data.push(key.unwrap())
        }
        data
    }

    #[tokio::main]
    #[test]
    async fn test_scan() {
        let mut db = get_db("scan").await;
        let iter = db.scan("*").await.unwrap();
        for item in collect(iter).await {
            println!("removed item: {:?}", String::from_utf8_lossy(&item));
            db.remove(item).await.unwrap();
        }
        println!("test_scan db_size: {:?}", db.db_size().await);
        db.insert("foo/abcd/1", &1).await.unwrap();
        db.insert("foo/abcd/2", &2).await.unwrap();
        db.insert("foo/abcd/3", &3).await.unwrap();
        db.insert("foo/abcd/**/4", &11).await.unwrap();
        db.insert("foo/abcd/*/4", &22).await.unwrap();
        db.insert("foo/abcd/*", &33).await.unwrap();
        db.insert("iot/abcd/5/a", &5).await.unwrap();
        db.insert("iot/abcd/6/b", &6).await.unwrap();
        db.insert("iot/abcd/7/c", &7).await.unwrap();
        db.insert("iot/abcd/", &8).await.unwrap();
        db.insert("iot/abcd", &9).await.unwrap();

        println!("test_scan db_size: {:?}", db.db_size().await);

        let format_topic = |t: &str| -> Cow<'_, str> {
            if t.len() == 1 {
                if t == "#" || t == "+" {
                    return Cow::Borrowed("*");
                }
            }
            let t = t.replace("*", "\\*").replace("?", "\\?").replace("+", "*");
            if t.len() > 1 && t.ends_with("/#") {
                Cow::Owned([&t[0..(t.len() - 2)], "*"].concat())
            } else {
                Cow::Owned(t)
            }
        };

        //foo/abcd*
        let topic = format_topic("foo/abcd/#");
        println!("topic: {}", topic);
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        let items = collect(iter).await;
        for item in items.iter() {
            println!("item: {:?}", String::from_utf8_lossy(&item));
        }
        assert_eq!(items.len(), 6);

        //"foo/abcd/\\**"
        let topic = format_topic("foo/abcd/*/#");
        println!("---topic: {} {}---", topic, "foo/abcd/\\**");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 3);

        //"foo/abcd/\\*"
        let topic = format_topic("foo/abcd/*");
        println!("---topic: {} {}---", topic, "foo/abcd/\\*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 1);

        //foo/abcd/*/*
        let topic = format_topic("foo/abcd/+/#");
        println!("---topic: {} {}---", topic, "foo/abcd/*/*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 6);

        //iot/abcd*
        let topic = format_topic("iot/abcd/#");
        println!("---topic: {} {}---", topic, "iot/abcd*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 5);

        //iot/abcd/+
        let topic = format_topic("iot/abcd/+");
        println!("---topic: {} {}---", topic, "iot/abcd/*");
        let iter = db.scan(topic.as_bytes()).await.unwrap();
        assert_eq!(collect(iter).await.len(), 4);
    }
}

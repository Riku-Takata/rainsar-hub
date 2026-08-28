# E:\shuron にコピーした MySQL データの起動手順

## 配置

MySQL のデータディレクトリ一式を以下にコピーしています。

```text
E:\shuron\mysql_data
```

元データは Docker Compose の bind mount で使われていた以下のディレクトリです。

```text
D:\sotsuron\mysql
```

`mysql.sock` は Windows 側でコピーできない Linux ソケットファイルです。MySQL 起動時に再生成されるため、復元には不要です。

## 起動方法

`E:\shuron` に置いた `docker-compose.mysql.yml` を使います。

```powershell
cd E:\shuron
docker compose -f docker-compose.mysql.yml up -d
```

既定ではホスト側の `3308` 番ポートで起動します。元の `rainsarhub-db` が `3307` を使っていても同時に起動できます。

接続情報は以下です。

```text
host: 127.0.0.1
port: 3308
database: rainsar_hub
user: rainsar
password: rainsar_pw
root_password: root
```

## 接続確認

```powershell
docker exec -it shuron-mysql-db mysql -u rainsar -prainsar_pw rainsar_hub
```

件数確認の例です。

```sql
SELECT COUNT(*) FROM gsmap_points;
```

元環境で確認した `gsmap_points` は `28,618,205` 件でした。

## 停止方法

```powershell
cd E:\shuron
docker compose -f docker-compose.mysql.yml down
```

## 3307 番ポートで起動したい場合

元の `rainsarhub-db` が動いている場合は先に停止します。

```powershell
cd D:\sotsuron\rainsar-hub
docker compose --env-file infra\.env -f infra\docker-compose.yml stop db
```

その後、`E:\shuron` 側を `3307` で起動します。

```powershell
cd E:\shuron
$env:DB_PORT_HOST = "3307"
docker compose -f docker-compose.mysql.yml up -d
```

## 注意

この方法は MySQL 8 のデータディレクトリをそのまま使う方式です。別PCで使う場合も Docker Desktop と `mysql:8` イメージが必要です。

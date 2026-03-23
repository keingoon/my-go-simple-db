# my-go-simple-db

「Database Design and Implementation」を読みながら、Go でシンプルなデータベースを実装している学習用プロジェクトです。

## 現在の仕様

- ページ指向のストレージ管理を持つ
- Write-Ahead Logging (WAL) による更新ログを持つ
- トランザクションの `commit` / `rollback` をサポートする
- ARIES ベースの crash recovery を実装している
  - recovery は `analysis` / `redo` / `undo` の 3 phase で動作する
  - `Checkpoint` をログに書き込み、起動時 recovery の analysis 開始位置を絞る
  - dirty page table (DPT) と pageLSN を使って redo 範囲を絞る
- background page cleaner により dirty page を定期的に flush する
- `SimpleDB` が runtime owner として以下を提供する
  - `Start()`
  - `NewTransaction()`
  - `Close()`

## Recovery / Runtime

- 起動時に crash recovery を実行する
- runtime は定期的に checkpoint と dirty page flush を実行する
- `SimpleDB` は `Start()` / `NewTransaction()` / `Close()` を提供する

## 現状の位置づけ

- 学習と実験を目的にした実装であり、API や構成は今後変更される可能性がある
- 現在はストレージ、バッファ管理、トランザクション管理、recovery を中心に実装している
- 上位レイヤの機能は今後段階的に追加していく

## TODO

- レコード管理 (`Record Management`) を実装する
- メタデータ管理 (`Metadata Management`) を実装する
- 問い合わせ処理・パース・プランニング (`Query Processing` / `Parsing` / `Planning`) を実装する
- インデックス (`Indexing`) を実装する
- クエリ最適化 (`Query Optimization`) を実装する


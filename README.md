# gce-task-runner

## 概要

社内でよくある次のようなシーンで使えるライブラリです

* GCEインスタンスを起動して、何か処理をさせたい
* 複数インスタンスの分散処理で全インスタンスの処理が終わってから次の処理をさせたい
* タスクごとにスペックやGPUの有無を変えたい
* フリーズ時の無駄な課金を防ぐために時間制限を設けたい

行なっていることは主に次の2点です。

* タスクの実行を管理するマネージャー
  * 管理インスタンスにあたる。GCEで実行する必要はないのでローカルでも実行可能。
  * 起動スクリプトを指定したランナーにあたるインスタンスを起動する
  * ランナーからの`gce_task_runner.notify_completion()`をキャッチしてインスタンスを削除する
  * そのタスクのインスタンスが全て削除されたら、次のタスクに移動して繰り返す
      * エラーがあれば後続処理はしない

* タスクの実行を行うランナー
  * マネージャーが作成するGCEインスタンス
  * 行いたい処理はマネージャーが作成時に指定する起動スクリプトに書いておく
  * 処理が終わったら`gce_task_runner.notify_completion()`を呼び出ことでマネージャーに削除される
      * エラーがあればそれを含めることも可能

## 試しに実行してみる

[sample_manager.py](./sample_manager.py)を確認してください。  

```shell
# インストール
$ git clone https://github.com/COLORFULBOARD/gce_task_runner.git
$ cd gce_task_runner
$ python3 -m venv venv
$ . venv/bin/activate
(venv) $ pip install -e .

# 実行
(venv) $ python sample_manager.py
```


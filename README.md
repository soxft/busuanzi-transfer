# bsz_transfer

> 使用该工具可以将 busuanzi 迁移至 2.8.x
>
> 2.8.x 后续 UV 使用 hyperloglog 进行统计, 有效的降低了内存占用

## 注意

迁移前，请务必备份您的数据 (dump.rdb)

## 使用

1. 修改 config.yaml, prefix 为先前的 redis 前缀，ToPrefix 为新的 redis 前缀
2. `go run main.go` 或使用 release 中的 二进制文件
3. 下载新版本的 busuanzi, 并配置 config.yaml, 使得 

- prefix 为新的 redis 前缀
- Bsz.Encrypt = MD532
- Bsz.PathStyle = false

## 后续操作

在验证数据迁移完成后, 可手动删除旧前缀的数据释放内存

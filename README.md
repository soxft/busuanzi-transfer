# bsz_transfer

> 使用该工具可以将 busuanzi 迁移至 2.8.x


## 注意

迁移前，请务必备份您的数据 (dump.rdb)

## 使用

1. 修改 config.yaml, prefix 为先前的 redis 前缀，ToPrefix 为新的 redis 前缀
2. 允许该工具
3. 下载新版本的 busuanzi, 并配置 config.yaml, 使得 

- prefix 为新的 redis 前缀
- Bsz.Encrypt = MD532
- Bsz.PathStyle = false
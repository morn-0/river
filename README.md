# river - 高性能数据迁移工具

### 特性

* 高性能，基于流的形式迁移数据
* 支持多种文件以及多种数据库之间互相迁移
* 支持 Lua 脚本实现高自定义性操作

### 安装

#### 二进制文件
<https://github.com/cc-morning/river/releases>

#### 源码编译
```bash
git clone https://github.com/cc-morning/river.git
cd river
cargo build --release
```

### 使用教程

#### 设计说明
整个迁移过程就像一条河流

```
    // 直接迁移
    From Stream -> To Stream
     |              |
 e.g: Oracle    e.g: PostgreSQL

                    Lua 脚本
    // Lua 脚本迁移   /
    From Stream -> Filter Stream -> To Stream
     |                               |
 e.g: Oracle | JSON | CSV        e.g: MySQL | ES | PostgreSQL
```


#### 支持的类型
From：CSV，JSON，MySQL，Oracle，PostgreSQL

To：Elasticsearch，MySQL，Oracle，PostgreSQL


#### 示例
`config.toml`

```toml
[task1]
thread = 2 # 任务协程运行时的调度线程数
filter = "append_time.lua" # Lua 脚本: 追加一个当前时间字段

[task1.from.csv]
path = [ "/xxx/xxx/test1.csv", "/xxx/xxx/test2.csv" ]
delimiter = "," # 分隔符
flexible = true # 忽略错位
quoting = true # 支持引号
double_quote = true # 引号转义

[task1.to.postgresql]
url = "postgresql://test:test@127.0.0.1/test"
table = "test"
fixnul = true # 修复 0x00

[task2.from.json]
path = [ "/xxx/xxx/test1.json", "/xxx/xxx/test2.json" ]
columns = "id,name,sex,age"
parallel = 10240 # 格式化，提取指定 key 行为并行度

[task2.to.mysql]
url = "mysql://test:test@127.0.0.1/test"
table = "test"
```

`append_time.lua`

```lua
// 获取行数据
local row = ...

// 追加一个当前时间字段
row[#row + 1] = os.date("%Y-%m-%d %H:%M:%S", os.time())

// 返回处理后的当前行，若返回空数组则丢弃当前行
return row
```

多个任务会多线程同时执行。

运行命令

```bash
# // 不打印日志执行
# river config.toml
# 
# // 打印日志执行（日志级别：error，warn，info，debug，trace）
# LOG=info river config.toml
```

#### [查看完整配置](https://github.com/cc-morning/river/wiki/configuration)

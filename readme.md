# go微服务实践-基于go-zero实现一个数字货币交易平台。 

在线体验 斥巨资搞了台机器  http://47.113.223.16 

账户密码在这 https://github.com/ikun2021/gex/blob/main/resource/users.txt


后端：https://github.com/ikun2021/gex   您的star,点赞评论是我更新的动力

前端：https://github.com/ikun2021/gex-ui

基于go-zero 实现一个数字货币交易平台现货交易的基本功能。

- 限价单，市价单的撮合。
- 基本行情(盘口，k线，tick)，以及个人订单变化的实时推送。

## 基本架构



![img](https://cdn.nlark.com/yuque/0/2024/png/12466223/1733649642983-07a74740-d89c-4c95-90b7-a038fd4cbe95.png)

## 基本功能

### 限价单
![](https://cdn.learnku.com/uploads/images/202406/10/51993/bZZs8Xnchx.gif)

### 市价单

![](https://cdn.learnku.com/uploads/images/202406/10/51993/vVNUSmI7Pp.gif)



## 运行项目

项目依赖的中间件：消息组件pulsar，数据库mysql,redis，分布式事务dtm，websocket推送gpush。

1、配置一个host 映射， api.gex.com:项目地址

2、项目已经整理好docker-compose文件，依赖和程序分别在不同的docker-compose文件，使用docker-compose即可一键启动项目，docker版本不能太旧具体如下。

```shell
root@ubuntu:~/smb# docker-compose -v
Docker Compose version v2.6.1

root@ubuntu:~/smb# docker version
Client: Docker Engine - Community
 Version:           24.0.6
 API version:       1.43
 Go version:        go1.20.7
 Git commit:        ed223bc
 Built:             Mon Sep  4 12:32:12 2023
 OS/Arch:           linux/amd64
 Context:           default
 
执行命令 
make build 编译项目。
make run 启动项目。
make clear 删除镜像和容器（会删除所有的容器和新建的镜像。）

账号lisi     密码lisilisi
 
```

3、直接访问启动项目机器的ip， 默认nginx配置的是的是80端口。



## go 实践

### go-zero api rpc 基本使用

### dtm saga处理分布式事务。

场景：分布式事务的问题，下单需要在订单服务插入一条数据，同时需要在账户服务冻结对应的资产。

解决方法：使用dtm的saga模式解决了分布式事务的问题。

### etcd 实现管理后台动态配置参数。

场景：服务使用的一些配置，需要动态配置，在管理后台修改后，要及时生效。如价格精度，具体错误码的message。

解决方法：使用etcd的动态配置。

### ast语法树 自制工具生成错误码，完善gorm gen的软删除

在一些场景如工具生成的代码无法满足我们的需要，或需要根据一段代码生成另一段代码的时候，可以使用ast语法树自制工具。 

### 错误日志在我们的上传到我们的im中如lark,tg,企业微信。

新增一个zap core即可，将指定级别的日志输出到我们的im中具体参考

https://github.com/ikun2021/zlog

### Docke部署

使用docker-compose部署

## v1.3.0 

待完成

1、完善前端 。

2、k8s部署。

3、搞台服务器部署。

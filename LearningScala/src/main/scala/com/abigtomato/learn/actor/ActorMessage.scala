package actor

// Worker节点发送给Master节点的注册消息
case class RegisterWorker(id: String, workerHost: String, memory: String, cores: String)

// Worker节点发送给Master节点的心跳消息
case class HeartBeat(workerId: String)

// Master节点检查Worker节点超时离线的消息
case class CheckOfTimeOutWorker()

// Master节点对于Worker节点注册消息的ACK应答
case class RegisteredWorker(workerId: String)

// Worker节点周期性的发送心跳消息给自己
case class SendHeartBeat()
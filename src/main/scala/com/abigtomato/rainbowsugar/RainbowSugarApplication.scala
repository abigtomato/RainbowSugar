package com.abigtomato.rainbowsugar

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.context.annotation.ComponentScan

@SpringBootApplication
class RainbowSugarApplication

@ComponentScan(value = Array("com.abigtomato.rainbowsugar.controller"))
@EntityScan(value = Array("com.abigtomato.rainbowsugar.entity"))
object RainbowSugarApplication extends App {

  SpringApplication.run(classOf[RainbowSugarApplication])
}

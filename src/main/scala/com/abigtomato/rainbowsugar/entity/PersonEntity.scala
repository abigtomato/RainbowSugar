package com.abigtomato.rainbowsugar.entity

import javax.persistence._

import scala.beans.BeanProperty

@Entity
//@Table(name = "person")
class PersonEntity {

  @Id
  @GeneratedValue
  @BeanProperty
  var id: Integer = _

  @BeanProperty
  var name: String = _

  @BeanProperty
  var sex: String = _
}

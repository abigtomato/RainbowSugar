package com.abigtomato.rainbowsugar.controller

import com.abigtomato.rainbowsugar.entity.PersonEntity
import com.abigtomato.rainbowsugar.service.PersonService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.{Page, PageRequest}
import org.springframework.web.bind.annotation.{GetMapping, PathVariable, PostMapping, RequestBody, RequestMapping, RestController}

@RestController
@RequestMapping(value = Array("/v1/person"))
class PersonController @Autowired() (private val personService: PersonService) {

  @PostMapping(value = Array("/save"))
  def save(@RequestBody person: PersonEntity): Integer = personService.save(person).id

  @GetMapping(value = Array("/{id}"))
  def selectPersonById(@PathVariable id: Integer): PersonEntity = personService.selectPersonById(id)

  @GetMapping(value = Array("/list"))
  def getAll: Page[PersonEntity] = this.personService.getAll(PageRequest.of(0, 10))
}

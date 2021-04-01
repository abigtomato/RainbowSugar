package com.abigtomato.rainbowsugar.service.impl

import com.abigtomato.rainbowsugar.entity.PersonEntity
import com.abigtomato.rainbowsugar.repository.PersonRepository
import com.abigtomato.rainbowsugar.service.PersonService
import com.abigtomato.rainbowsugar.support.PersonSupport
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.{Page, Pageable}
import org.springframework.stereotype.Service

@Service
class PersonServiceImpl @Autowired() (personSupport: PersonSupport) extends PersonService {
  /**
   *
   * @param person
   * @return
   */
  override def save(person: PersonEntity): PersonEntity = this.personSupport.save(person)

  /**
   *
   * @param id
   * @return
   */
  override def selectPersonById(id: Integer): PersonEntity = this.personSupport.findById(id).get()

  /**
   *
   * @param page
   * @return
   */
  override def getAll(page: Pageable): Page[PersonEntity] = this.personSupport.findAll(page)
}

package com.abigtomato.rainbowsugar.service

import com.abigtomato.rainbowsugar.entity.PersonEntity
import org.springframework.data.domain.{Page, Pageable}
import org.springframework.transaction.annotation.Transactional

trait PersonService {

  /**
   *
   * @param person
   * @return
   */
  @Transactional
  def save(person: PersonEntity): PersonEntity

  /**
   *
   * @param id
   * @return
   */
  def selectPersonById(id: Integer): PersonEntity

  /**
   *
   * @param page
   * @return
   */
  def getAll(page: Pageable): Page[PersonEntity]
}

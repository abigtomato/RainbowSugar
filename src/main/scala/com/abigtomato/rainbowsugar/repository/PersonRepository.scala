package com.abigtomato.rainbowsugar.repository

import com.abigtomato.rainbowsugar.entity.PersonEntity
import org.springframework.data.repository.CrudRepository

trait PersonRepository extends CrudRepository[PersonEntity, Integer] {
}

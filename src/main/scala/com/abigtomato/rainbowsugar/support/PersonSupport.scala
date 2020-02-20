package com.abigtomato.rainbowsugar.support

import com.abigtomato.rainbowsugar.entity.PersonEntity
import org.springframework.data.repository.PagingAndSortingRepository

trait PersonSupport extends PagingAndSortingRepository[PersonEntity, Integer] {
}

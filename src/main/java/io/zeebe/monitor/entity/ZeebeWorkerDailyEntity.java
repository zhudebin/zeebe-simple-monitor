/*
 * Copyright © 2017 camunda services GmbH (info@camunda.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.zeebe.monitor.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;

@Entity(name = "ZEEBE_WORKER_DAILY")
public class ZeebeWorkerDailyEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "id_")
  private long id;

  @Column(name = "type_")
  private String type;

  @Column(name = "name_")
  private String name;

  @Column(name = "day_")
  private String day;

  @Column(name = "TIMESTAMP_")
  private long timestamp;

  public long getId() {
    return id;
  }

  public void setId(final long id) {
    this.id = id;
  }

  public String getType() {
    return type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public String getDay() {
    return day;
  }

  public void setDay(final String day) {
    this.day = day;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(final long timestamp) {
    this.timestamp = timestamp;
  }
}

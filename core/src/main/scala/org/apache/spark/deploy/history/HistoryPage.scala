/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.history

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.ui.{UITableBuilder, UITable, WebUIPage, UIUtils}

private[spark] class HistoryPage(parent: HistoryServer) extends WebUIPage("") {

  private val pageSize = 20

  val appTable: UITable[ApplicationHistoryInfo] = {
    val t = new UITableBuilder[ApplicationHistoryInfo]()
    t.col("App ID") (identity) withMarkup { info =>
      val uiAddress = HistoryServer.UI_PATH_PREFIX + s"/${info.id}"
      <a href={uiAddress}>{info.id}</a>
    }
    t.col("App Name") { _.name }
    t.epochDateCol("Started") { _.startTime }
    t.epochDateCol("Completed") { _.endTime }
    t.durationCol("Duration") { info => info.endTime - info.startTime }
    t.col("Spark User") { _.sparkUser }
    t.epochDateCol("Last Updated") { _.lastUpdated }
    t.build()
  }

  def render(request: HttpServletRequest): Seq[Node] = {
    val requestedPage = Option(request.getParameter("page")).getOrElse("1").toInt
    val requestedFirst = (requestedPage - 1) * pageSize

    val allApps = parent.getApplicationList()
    val actualFirst = if (requestedFirst < allApps.size) requestedFirst else 0
    val apps = allApps.slice(actualFirst, Math.min(actualFirst + pageSize, allApps.size))

    val actualPage = (actualFirst / pageSize) + 1
    val last = Math.min(actualFirst + pageSize, allApps.size) - 1
    val pageCount = allApps.size / pageSize + (if (allApps.size % pageSize > 0) 1 else 0)

    val appTable = this.appTable.render(apps)
    val providerConfig = parent.getProviderConfig()
    val content =
      <div class="row-fluid">
        <div class="span12">
          <ul class="unstyled">
            {providerConfig.map { case (k, v) => <li><strong>{k}:</strong> {v}</li> }}
          </ul>
          {
            if (allApps.size > 0) {
              <h4>
                Showing {actualFirst + 1}-{last + 1} of {allApps.size}
                <span style="float: right">
                  {if (actualPage > 1) <a href={"/?page=" + (actualPage - 1)}>&lt;</a>}
                  {if (actualPage < pageCount) <a href={"/?page=" + (actualPage + 1)}>&gt;</a>}
                </span>
              </h4> ++
              appTable
            } else {
              <h4>No Completed Applications Found</h4>
            }
          }
        </div>
      </div>
    UIUtils.basicSparkPage(content, "History Server")
  }
}

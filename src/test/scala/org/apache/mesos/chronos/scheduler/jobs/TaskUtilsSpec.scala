package org.apache.mesos.chronos.scheduler.jobs

import org.joda.time._
import org.specs2.mock._
import org.specs2.mutable._

class TaskUtilsSpec extends SpecificationWithJUnit with Mockito {

  "TaskUtils" should {
    "Get taskId" in {
      val schedule = "R/2012-01-01T00:00:01.000Z/P1M"
      val arguments = "-a 1 -b 2"
      val job1 = new ScheduleBasedJob(schedule, "sample-name", "sample-command", arguments = List(arguments))
      val job2 = new ScheduleBasedJob(schedule, "sample-name", "sample-command")
      val ts = 1420843781398L
      val due = new DateTime(ts)

      val taskIdOne = TaskUtils.getTaskId(job1, due, 0)
      val taskIdTwo = TaskUtils.getTaskId(job2, due, 0)

      taskIdOne must_== "ct_-_1420843781398_-_0_-_sample-name_-_" + arguments
      taskIdTwo must_== "ct_-_1420843781398_-_0_-_sample-name_-_"
    }

    "Get job arguments for taskId" in {
      val arguments = "-a 1 -b 2"
      var taskId = "ct_-_1420843781398_-_0_-_test_-_" + arguments
      val jobArguments = TaskUtils.getJobArgumentsForTaskId(taskId)

      jobArguments must_== arguments
    }

    "Parse taskId" in {
      val arguments = "-a 1 -b 2"
      val arguments2 = "-a 1:2 --B test"

      val taskIdOne = "ct_-_1420843781398_-_0_-_test_-_" + arguments
      val (jobName, jobDue, attempt, jobArguments) = TaskUtils.parseTaskId(taskIdOne)

      jobName must_== "test"
      jobDue must_== 1420843781398L
      attempt must_== 0
      jobArguments must_== arguments

      val taskIdTwo = "ct_-_1420843781398_-_0_-_test_-_" + arguments2
      val (_, _, _, jobArguments2) = TaskUtils.parseTaskId(taskIdTwo)

      jobArguments2 must_== arguments2
    }
  }
}


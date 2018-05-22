package com.elite.wifi.manager.business.shedule;

import java.rmi.RemoteException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.CronExpression;
import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;

import com.elite.wifi.manager.comm.spring.SpringUtils;
import com.elite.wifi.manager.constant.JobConstants;
import com.elite.wifi.manager.dbservice.job.JobService;
import com.elite.wifi.manager.dbservice.job.WorkProcessService;
import com.elite.wifi.manager.persistent.eo.job.JobTable;
import com.elite.wifi.manager.persistent.eo.job.WorkProcess;

public class JobScheduleService {
	private final static Logger log = Logger.getLogger(JobScheduleService.class
			.getName());
	public static final SimpleDateFormat sdf = new SimpleDateFormat(
			"MM/dd/yyyy HH:mm:ss");
	public static final SimpleDateFormat sdfDate = new SimpleDateFormat(
			"MM/dd/yyyy");
	private SchedulerLauncher schedulerLauncher;

	public JobScheduleService() throws Exception {
		schedulerLauncher = SchedulerLauncher.getInstance();
		if (!schedulerLauncher.isStart()) {
			schedulerLauncher.launchScheduler();
		}
	}

	/**
	 * Add a job to Quartz.
	 * 
	 * @param jobEntry
	 */
	public void addAndScheduleJob(EliteJobEntry jobEntry) throws Exception {
		// add the job
		Scheduler scheduler = schedulerLauncher.getScheduler();
		JobDetail jobDetail = getJobDetail(jobEntry);
		try {
			scheduler.addJob(jobDetail, true);
			sheduleJob(jobEntry);
		} catch (Exception e) {
			log.error(e.getStackTrace());
			//e.printStackTrace();
			throw e;
		}
	}

	public void addAndScheduleJob(Long jobID) throws Exception {
		// add the job
		try{
		addAndScheduleJob(jobID,true);
		}catch(Exception ex){
			//ex.printStackTrace();
			throw ex;
		}

	}
	public Long immediatelyExecuteJob(Long jobID) throws Exception {
		// add the job
		try{
		addAndScheduleJob(jobID,true);
		int i=0;
		List<WorkProcess> process=null;
		while(i++<60){
		try {
			WorkProcessService service = (WorkProcessService) SpringUtils.getBean("workProcessService");
			process=service.getWorkProcessByJob(jobID);
			if(process!=null && process.size()>0){
				return process.get(0).getWorkId();
			}
			Thread.sleep(1*1000);
			
		} catch (Exception ex) {
			log.error("Failed to get JobService", ex);
			
		}
		}
		}catch(Exception ex){
			//ex.printStackTrace();
			throw ex;
		}
		return null;

	}
	public void addAndScheduleJob(Long jobID,boolean shouldChangeStatus) throws Exception {
		if(!shouldChangeStatus){
		// add the job
		EliteJobEntry jobEntry = null;

		jobEntry = JobEntryFactory.getInstance().rtrvJobEntry(jobID.toString());
		addAndScheduleJob(jobEntry);
		}else{
			JobTable job=JobEntryFactory.getInstance().loadJob(jobID);
			Boolean jobEnabled = job.getEnable();
			Integer executionType = job.getExecutionType();
			Integer jobStatus = job.getStatus();
			Date startDate = job.getStartDateTime();
			if (jobEnabled == null || !jobEnabled) {
				log.warn("Job enabled is false:"+job.getJobName());
				return;
			}
			if (executionType == null) {
				log.warn("Job execution type is null:"+job.getJobName());
				return;
			}
			JobService jobService = null;
			try {
				jobService = (JobService) SpringUtils.getBean("jobService");
			} catch (Exception ex) {
				log.error("Failed to get JobService", ex);
				return;
			}
			if (executionType == 1) {
				// Immediately execute,ignore it.
				addAndScheduleJob(jobID,false);
			} else if (executionType == 2) {
				// Once_Sheduled execute
				
				if (jobStatus == null) {
					jobStatus = 0;// waiting to run
				}
				switch (jobStatus) {
				case 1:
					// successfully complete 
					
				case 2:
					// failed complete 
					
				case 3:
					// partial complete
					
				case 6:
					// Expired
				case 0:
					// waiting to run
					
				
				case 4:
					// scheduled
					
				case 5:
					// in progressing
					
					//diff the started time and now
					Date nextStartDate = startDate;
					
					if(nextStartDate!=null && nextStartDate.getTime()<=new java.util.Date().getTime()){
						//set the job status is expired
						//job.setStatus(new Short("6"));
						Integer oldStatus=job.getStatus();
						if(oldStatus==null || (oldStatus!=JobConstants.JOB_STATUS_Success && oldStatus!=JobConstants.JOB_STATUS_PartiallySuccess && oldStatus!=JobConstants.JOB_STATUS_Failure)){
						try{
						jobService.updateJob(job.getJobId(), new String[]{"status","enable","nextExecDate"}, new Object[]{JobConstants.JOB_STATUS_Expired,new Boolean(false),null});
						//JobManager.setJobHisToExpired(jobService,job.getJobId());
						}catch(Exception ex){
							log.error(ex);
						}
						}
					}else if(nextStartDate!=null && nextStartDate.getTime()>new java.util.Date().getTime()){
						//set the job status is expired
						//job.setStatus(new Short("6"));
						try{
						jobService.updateJob(job.getJobId(), new String[]{"status","nextExecDate"}, new Object[]{JobConstants.JOB_STATUS_Init,nextStartDate});
						}catch(Exception ex){
							log.error(ex);
						}
						try {
							new JobScheduleService().addAndScheduleJob(job.getJobId(),false);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							log.error(e);
						}
						
						
					}
						
					
					break;
				
				}
			} else if (executionType == 3) {
				// Recurrence execute
				String endType=job.getEndType();
				/**0 no End, 1 end by endDate*/
				if(endType!=null && endType.trim().endsWith("1")){
					
				}
				Date endDate=job.getEndDate();
				
			    if(endDate!=null){
			    	String startAppoint=startDate.getHours()+":"+startDate.getMinutes()+":00";
				try {
					endDate = JobScheduleService.sdf
							.parse(JobScheduleService.sdfDate
									.format(endDate)
									+ " " + startAppoint);
				} catch (Exception e) {
					//e.printStackTrace();
					log.error(e);

				}
			    }
			    Integer oldStatus=job.getStatus();
				//System.out.println("job old status:"+oldStatus);
				if(endDate!=null && endDate.getTime()<new java.util.Date().getTime()){
					//set the job status is expired
					//job.setStatus(new Short("6"));
					
					if(oldStatus==null || (oldStatus!=JobConstants.JOB_STATUS_Success && oldStatus!=JobConstants.JOB_STATUS_PartiallySuccess && oldStatus!=JobConstants.JOB_STATUS_Failure)){
					try{
					jobService.updateJob(job.getJobId(), new String[]{"status","enable","nextExecDate"}, new Object[]{JobConstants.JOB_STATUS_Expired,new Boolean(false),null});
					}catch(Exception ex){
						log.error(ex);
					}
					}
				}else{
					try {
						new JobScheduleService().addAndScheduleJob(job.getJobId(),false);
					} catch (Exception e) {
						// TODO Auto-generated catch block
						//e.printStackTrace();
						log.error(e);
					}
					Integer jobType=job.getBusinessType();
					
					if(jobType!=null){
						Date nextTime=getNextFireTime(job.getJobId()+"", jobType.toString());
						//System.out.println("nextTime:"+nextTime);
						try{
							jobService.updateJob(job.getJobId(), new String[]{"nextExecDate"}, new Object[]{nextTime});
							}catch(Exception ex){
								//ex.printStackTrace();
								log.error(ex);
							}
					}
				}
				
				
			}
		}

	}
	public void deleteJob(Long jobID) throws Exception {
		EliteJobEntry jobEntry = JobEntryFactory.getInstance().rtrvJobEntry(jobID.toString());
		deleteJob(jobEntry);
	}
	

	/**
	 * Delete a specific job and all related triggers from Quartz.
	 * 
	 * @param jobEntry
	 */
	public void deleteJob(EliteJobEntry jobEntry) throws Exception {
		try {
			getScheduler().deleteJob(jobEntry.getJobId().toString(),
					jobEntry.getGroup());
		} catch (SchedulerException e) {
			log.error(e.getStackTrace());
			throw e;
		}
	}

	public void deleteJob(Long jobId, String group) throws Exception {
		try {
			getScheduler().deleteJob(jobId.toString(), group);
		} catch (SchedulerException e) {
			log.error(e.getStackTrace());
			throw e;
		}
	}

	public void executeJob(EliteJobEntry jobEntry) throws Exception {
		
		
		// check the job first.
		String jobId="";
		if(jobEntry.getJobId()!=null){
			jobId= jobEntry.getJobId().toString();
		}else{
			jobId=new java.util.Date().getTime()+"";
		}
		String group = jobEntry.getGroup();
		if (jobId != null) {
			JobDetail jobDetail = null;
			Scheduler scheduler = schedulerLauncher.getScheduler();
			try {
				jobDetail =scheduler.getJobDetail(jobId, group);
				if (jobDetail == null) {
					jobDetail = getJobDetail(jobEntry);
					if (jobDetail != null) {
						scheduler.addJob(jobDetail, true);
					}
				}

				getScheduler().triggerJobWithVolatileTrigger(jobId, group);
			} catch (SchedulerException e) {
				log.error(e.getStackTrace());
				throw e;
			}
		}
	}

	/**
	 * Schedule a job using Quartz.
	 * 
	 * @param jobEntry
	 * @throws Exception
	 */
	public void sheduleJob(EliteJobEntry jobEntry) throws Exception {
		// System.out.println("Schedule the job");
		// Map<String, Object> jobData = jobEntry.getJobData();
		// jobData.remove(JobEntryDef.nextExecutionTime);

		boolean jobEnabled = jobEntry.getJobEnabled();
		if (!jobEnabled) {
			
			return;
		}
		java.util.List<_TriggerEntry> triggerEntries = jobEntry
				.getTriggerEntries();
		JobScheduleTypeEnum scheduleType = JobScheduleTypeEnum
				.converToEnum(jobEntry.getJobData().getJobScheduleType());

		if (scheduleType.equals(JobScheduleTypeEnum.Immediately)) {
			// schedule the immediately job
			try {
				getScheduler().triggerJobWithVolatileTrigger(
						jobEntry.getJobId().toString(), jobEntry.getGroup());
			} catch (SchedulerException e) {
				log.error(e.getStackTrace());
				throw e;
			}
		} else if (scheduleType.equals(JobScheduleTypeEnum.Once_Sheduled)) {
			// schedule the once job with appointment time.
			if (triggerEntries == null || triggerEntries.size() < 1) {
				log.error("Ignore the once job as the job trigger is empty.");
				return;
			}
			String triggerId = triggerEntries.get(0).getJobId();
			Date startDate = triggerEntries.get(0).getStartDate();
			String appointmentTime = triggerEntries.get(0).getAppointmentTime();
			Date sd = null;
			try {
				sd = sdf.parse(sdfDate.format(startDate) + " "
						+ appointmentTime.trim() + ":00");
			} catch (ParseException e) {
				log.error(e.getStackTrace());
				throw e;
			}
			if (sd.before(Calendar.getInstance().getTime())) {
				return;
			}
			SimpleTrigger st = new SimpleTrigger(triggerId,
					jobEntry.getGroup(), sd);
			st.setJobGroup(jobEntry.getGroup());
			st.setJobName(jobEntry.getJobId().toString());
			try {
				getScheduler().scheduleJob(st);
			} catch (SchedulerException e) {
				log.error(e.getStackTrace());
				throw e;
			}
		} else if (scheduleType.equals(JobScheduleTypeEnum.Recurrence)) {

			List<Date> nextExecutionTimeList = new ArrayList<Date>();
			Long repeatInvObj = null;// triggerEntryMap.get(TriggerEntryDef.repeatInterval);
			for (_TriggerEntry triggerEntry : triggerEntries) {
				
				Date startDate = triggerEntry.getStartDate();
				Date endDate = triggerEntry.getEndDate();

				Date currentTime = Calendar.getInstance().getTime();
				/*
				if (startDate == null || startDate.before(currentTime)) {
					startDate = currentTime;
				}
				*/
				if (startDate == null ) {
					startDate = currentTime;
				}
				RecurrencePatternEnum rcPattern = RecurrencePatternEnum.converToEnum(triggerEntry.getRecurrencePattern());
				if(rcPattern==RecurrencePatternEnum.DAILY){
					Integer days=triggerEntry.getEveryDays();
					if(days>2){
						String s="";
					}
					if(days==null || days<1){
						log.warn("Invalid job");
						continue;
					}else{
						repeatInvObj=new Long(days.toString())*24*60*60*1000;
					}
					if (startDate != null && startDate.before(currentTime)) {
						if(currentTime.getHours()>startDate.getHours() || (currentTime.getHours()==startDate.getHours() && currentTime.getMinutes()>startDate.getMinutes()) || (currentTime.getHours()==startDate.getHours() && currentTime.getMinutes()==startDate.getMinutes() && currentTime.getSeconds()>startDate.getSeconds())){
							//add 1 day
							currentTime.setTime(currentTime.getTime()+1*24*60*60*1000);
						}
						currentTime.setHours(startDate.getHours());
						currentTime.setMinutes(startDate.getMinutes());
						currentTime.setSeconds(startDate.getSeconds());
						long deltaDays=(currentTime.getTime()-startDate.getTime())/(1000*60*60*24);
						if(deltaDays % days==0){
							startDate=currentTime;
						}else{
							startDate=new java.util.Date(currentTime.getTime()+(days-deltaDays%days)*24*60*60*1000);
						}
						
						//startDate = currentTime;
					}
				}
				if(rcPattern==RecurrencePatternEnum.HOURLY){
					Integer hours=triggerEntry.getEveryHours();
					if(hours==null || hours<1){
						log.warn("Invalid job");
						continue;
					}else{
						repeatInvObj=new Long(hours*60*60*1000);
					}
					
					if (startDate != null && startDate.before(currentTime)) {
						if(currentTime.getMinutes()>startDate.getMinutes() || (currentTime.getMinutes()==startDate.getMinutes() && currentTime.getSeconds()>startDate.getSeconds())){
							//add 1 hour
							currentTime.setTime(currentTime.getTime()+1*60*60*1000);
						}
						currentTime.setMinutes(startDate.getMinutes());
						currentTime.setSeconds(startDate.getSeconds());
						long deltaHours=(currentTime.getTime()-startDate.getTime())/(1000*60*60);
						if(deltaHours % hours==0){
							startDate=currentTime;
						}else{
							startDate=new java.util.Date(currentTime.getTime()+(hours-deltaHours%hours)*60*60*1000);
						}
						
						//startDate = currentTime;
					}
					
				}
				
				String triggerId = triggerEntry.getJobId();// (String)triggerEntryMap.get(TriggerEntryDef.ID);
				if (repeatInvObj != null && (Long) repeatInvObj > 0) {
					// schedule recurrence job with repeated interval.
					
					SimpleTrigger st=new SimpleTrigger(triggerId,jobEntry.getGroup());
					st.setStartTime(startDate);
					// interval unit is second
					st.setRepeatInterval(repeatInvObj);
					st.setRepeatCount(SimpleTrigger.REPEAT_INDEFINITELY);
					if(endDate!=null){
						st.setEndTime(endDate);
					}
					st.setJobName(jobEntry.getJobId().toString());
					st.setJobGroup(jobEntry.getGroup());
					
					try {
						getScheduler().scheduleJob(st);
					} catch (SchedulerException e) {
						log.error(e.getStackTrace());
						throw e;
					}
					// add the next execution time.
					nextExecutionTimeList.add(st.getNextFireTime());
				} else {
					// schedule recurrence job with cron expression.
					CronExpression ce = null;
					try {
						String expression = jobEntry
								.getCronExpression(triggerEntry);
						ce = new CronExpression(expression);
					} catch (ParseException e) {
						log.error(e.getStackTrace());
						throw e;
					}

					CronTrigger ct = null;

					try {
						ct = new CronTrigger(triggerId, jobEntry.getGroup(), ce
								.getCronExpression());
					} catch (ParseException e) {
						log.error(e.getStackTrace());
						throw e;
					}
					if (startDate == null || startDate.before(currentTime)) {
						startDate = currentTime;
					}
					if (startDate != null)
						ct.setStartTime(startDate);
					if (endDate != null)
						ct.setEndTime(endDate);
					ct.setJobName(jobEntry.getJobId().toString());
					ct.setJobGroup(jobEntry.getGroup());
					try {
						getScheduler().scheduleJob(ct);
					} catch (SchedulerException e) {
						log.error(e.getStackTrace());
						throw e;
					}
					nextExecutionTimeList.add(ct.getNextFireTime());
				}
			}
			// update the job entry info of next execution time.
			Date nextExecutionTime = null;
			for (Date et : nextExecutionTimeList) {
				if (nextExecutionTime == null || et.before(nextExecutionTime)) {
					nextExecutionTime = et;
				}
			}
			if (nextExecutionTime != null) {
				// Map dataMap = new HashMap();
				// jobEntry.getJobData().put(JobEntryDef.nextExecutionTime, new
				// Long(nextExecutionTime.getTime()).toString());
				jobEntry.getJobData().setNextExecutionTime(
						nextExecutionTime.toString());
			}
		}
	}

	/**
	 * Unschedule job - remove all related triggers.
	 * 
	 * @param jobEntry
	 */
	public void unScheduleJob(String triggerName, String group)
			throws Exception {
		// TODO
		log.debug("TODO: Unimplemented method " + this.getClass()
				+ "unScheduleJob().");
		if (triggerName != null) {
			try {
				getScheduler().unscheduleJob(triggerName, group);
				getScheduler().deleteJob(triggerName, group);
			} catch (SchedulerException e) {
				log.error(e);
				throw e;
			}
		}
	}

	public void unScheduleJob(EliteJobEntry jobEntry) throws Exception {
		java.util.List<_TriggerEntry> triggers = jobEntry.getTriggerEntries();
		for (_TriggerEntry trigger : triggers) {
			String triggerId = trigger.getJobId();
			String group = jobEntry.getGroup();
			if (triggerId != null) {
				unScheduleJob(triggerId, group);
			}
		}
	}

	/**
	 * Unschedule a job with a specific trigger.
	 * 
	 * @param jobEntry
	 * @param trigger
	 */
	public void unScheduleJob(EliteJobEntry jobEntry, Trigger trigger) {
		log.debug("TODO: Unimplemented method " + this.getClass()
				+ "unScheduleJob().");
	}

	/**
	 * Cancel job
	 * 
	 * @param jobEntry
	 */
	public void cancelJob(EliteJobEntry jobEntry) {
		// TODO
		log.debug("TODO: Unimplemented method " + this.getClass()
				+ "cancelJob().");
	}

	public void addJobListener(EliteJobEntry jobEntry, EliteJobListener listener) {
		// TODO
		log.debug("TODO: Unimplemented method " + this.getClass()
				+ "addJobListener().");
	}

	private Scheduler getScheduler() {
		return schedulerLauncher.getScheduler();
	}

	private JobDetail getJobDetail(EliteJobEntry jobEntry) {
		EliteJobDetail jobDetail = new EliteJobDetail();
		
		jobDetail.setScheduleFlag(jobEntry.getScheduleFlag());
		jobDetail.setJobEntry(jobEntry);
		jobDetail.setName(jobEntry.getJobId()+"");		
		jobDetail.setGroup(jobEntry.getGroup());
		jobDetail.setJobClass(jobEntry.getJobClass());
		//jobDetail.getJobDataMap().put("ID", jobEntry.getJobId());
		return jobDetail;
	}

	private List<Trigger> getJobTriggers(String jobId, String group)
			throws Exception {
		if (jobId == null)
			return null;
		List<Trigger> triggers = new ArrayList<Trigger>();
		try {
			Trigger[] rawTriggerList = getScheduler().getTriggersOfJob(jobId,
					group);
			for (Trigger trigger : rawTriggerList) {
				triggers.add(trigger);
			}
		} catch (SchedulerException e) {
			log.error(e.getStackTrace());
			throw new Exception(e.getMessage());
		}
		return triggers;
	}

	public  Date getNextFireTime(String jobId, String group) throws Exception {
		Scheduler scheduler = getScheduler();
		List<Trigger> triggers = getJobTriggers(jobId, group);
		Date nextFireTime = null;
		for (Trigger trigger : triggers) {
			Date rNextFireTime = trigger.getNextFireTime();
			if (rNextFireTime != null) {
				if (nextFireTime == null || rNextFireTime.before(nextFireTime)) {
					nextFireTime = trigger.getNextFireTime();
				}
			}
		}
		return nextFireTime;
	}

	public void reScheduleAllJobs() throws Exception {
		/*
		 * // reschedule the pm collection jobs. reSchedulePMCollectionJobs(); //
		 * reschedule all stored jobs. BOManager bom = BOUtil.getBOManager();
		 * List<Map> rList = null; try { rList = (List<Map>)bom.getByQueryConstraint("JobEntry",
		 * ""); } catch (RemoteException e) { log.error(e.getStackTrace());
		 * throw e; } if (rList != null) { for (Map m:rList) { String jobId =
		 * (String)m.get(JobEntryDef.ID);
		 * 
		 * JobEntryFactory jef = JobEntryFactory.getInstance(); EliteJobEntry je =
		 * jef.rtrvJobEntry(jobId); addAndScheduleJob(je);
		 * je.updateNextFireTime(); } }
		 */
	}

	private void reSchedulePMCollectionJobs() throws Exception {

	}
}

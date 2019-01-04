/*******************************************************************************
 * Copyright (c) 2019 Sebastian Palarus
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v20.html
 *
 * Contributors:
 *     Sebastian Palarus - initial API and implementation
 *******************************************************************************/
package org.sodeac.example.multichainlist;

public class Task
{
	private String jobDescription = null;
	private boolean takeover = false;
	private long durationInMs = 1000;
	
	public Task(String jobDescription)
	{
		super();
		this.jobDescription = jobDescription;
	}
	
	public String getJobDescription()
	{
		return jobDescription;
	}
	
	public long getDurationInMs()
	{
		return durationInMs;
	}

	public Task setDurationInMs(long durationInMs)
	{
		this.durationInMs = durationInMs;
		return this;
	}

	public Task.Control takeOverTask()
	{
		synchronized (this)
		{
			if(takeover)
			{
				return null;
			}
			takeover = true;
			return new Task.Control();
		}
	}
	
	private void free()
	{
		synchronized (this)
		{
			takeover = false;
		}
	}
	
	private void runBy(String chain)
	{
		Task.Logger.getInstance().logTaskRun(this, chain);
		try {Thread.sleep(durationInMs);}catch (Exception e) {}
	}

	public class Control
	{
		
		public Control()
		{
			super();
		}
		
		public void free()
		{
			Task.this.free();
		}
		
		public void runBy(String chain)
		{
			Task.this.runBy(chain);
		}
	}
	
	public static class Logger
	{
		public static Task.Logger logger;
		
		public static Task.Logger getInstance()
		{
			if(logger == null)
			{
				synchronized (Task.Logger.class)
				{
					if(logger == null)
					{
						Task.Logger.logger = new Logger();
					}
				}
			}
			return Task.Logger.logger;
		}
		
		public void logTaskRun(Task task, String chain)
		{
			synchronized (this)
			{
				System.out.println(chain  + ": " + task.getJobDescription());
			}
		}
	}
}

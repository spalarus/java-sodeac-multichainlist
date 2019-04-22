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
package org.sodeac.multichainlist;

/**
 * 
 * @author Sebastian Palarus
 * @since 1.0
 * @version 1.0
 *
 */
public class ListFullException extends RuntimeException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5465935666667736920L;
	
	private long maxSize;
	
	public ListFullException(long maxSize)
	{
		super("list can not contains more then " + maxSize +" items" );
	}

	public long getMaxSize()
	{
		return maxSize;
	}	

}

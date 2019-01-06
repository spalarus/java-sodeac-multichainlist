/*******************************************************************************
 * Copyright (c) 2018 Sebastian Palarus
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
public class ChainConflictException extends RuntimeException
{
	/**
	 * 
	 */
	private static final long serialVersionUID = 4718802784256615868L;
	
	private String alreadyInChain;
	private Node<?> node;
	
	public ChainConflictException(String alreadyInChain, Node<?> node)
	{
		super("chain " + alreadyInChain + " can not insert node , because it already exists " );
	}	

	public String getAlreadyInChain()
	{
		return alreadyInChain;
	}

	public Node<?> getNode()
	{
		return node;
	}
	
}

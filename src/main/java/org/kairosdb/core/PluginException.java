package org.kairosdb.core;

/**
 Created by bhawkins on 5/19/17.
 */
public class PluginException extends Exception
{
	private static final long serialVersionUID = 5159086578496853083L;

	public PluginException()
	{
	}

	public PluginException(String pluginName, String message)
	{
		super("Exception in "+pluginName+": "+message);
	}

	public PluginException(String pluginName, String message, Throwable cause)
	{
		super("Exception in "+pluginName+": "+message, cause);
	}
}

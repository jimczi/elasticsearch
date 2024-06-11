/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import java.security.BasicPermission;

/**
 * A permission granted to ensure secured access to a file specified by a setting in the config directory.
 * <p>
 * By granting this permission with a setting key (wildcards are supported),
 * the files pointed to by the settings are secured from general access by Elasticsearch and other Elasticsearch plugins.
 * All code that does not have a secured permission on the same file will be denied all read/write access to that file.
 * Note that you also need to wrap any access to secured files in an {@code AccessController.doPrivileged()} block
 * as Elasticsearch itself is denied access to files secured by plugins.
 */
public class SecuredConfigFileSettingAccessPermission extends BasicPermission {
    public SecuredConfigFileSettingAccessPermission(String setting) {
        super(setting, "");
    }
}

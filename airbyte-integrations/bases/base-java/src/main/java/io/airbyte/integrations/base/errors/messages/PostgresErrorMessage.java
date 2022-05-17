/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.base.errors.messages;

import static io.airbyte.integrations.base.errors.utils.ConnectionErrorType.INCORRECT_ACCESS_PERMISSION;
import static io.airbyte.integrations.base.errors.utils.ConnectionErrorType.INCORRECT_DB_NAME;
import static io.airbyte.integrations.base.errors.utils.ConnectionErrorType.INCORRECT_HOST_OR_PORT;
import static io.airbyte.integrations.base.errors.utils.ConnectionErrorType.INCORRECT_SCHEMA_NAME;
import static io.airbyte.integrations.base.errors.utils.ConnectionErrorType.INCORRECT_USERNAME_OR_PASSWORD;
import static io.airbyte.integrations.base.errors.utils.ConnectorType.POSTGRES;

import io.airbyte.integrations.base.errors.utils.ConnectorType;

public class PostgresErrorMessage extends ErrorMessage {

  {
    CONSTANTS.put("28P01", INCORRECT_USERNAME_OR_PASSWORD);
    CONSTANTS.put("42501", INCORRECT_ACCESS_PERMISSION);
    CONSTANTS.put("08001", INCORRECT_HOST_OR_PORT);
    CONSTANTS.put("3D000", INCORRECT_DB_NAME);
    CONSTANTS.put("3F000", INCORRECT_SCHEMA_NAME);
  }

  @Override
  public ConnectorType getConnectorType() {
    return POSTGRES;
  }

}
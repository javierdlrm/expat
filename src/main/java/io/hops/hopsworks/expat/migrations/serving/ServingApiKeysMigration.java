/**
 * This file is part of Expat
 * Copyright (C) 2021, Logical Clocks AB. All rights reserved
 * <p>
 * Expat is free software: you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 * <p>
 * Expat is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Affero General Public License for more details.
 * <p>
 * You should have received a copy of the GNU Affero General Public License along with
 * this program. If not, see <https://www.gnu.org/licenses/>.
 */

package io.hops.hopsworks.expat.migrations.serving;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.DbConnectionFactory;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariables;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariablesFacade;
import io.hops.hopsworks.expat.kubernetes.KubernetesClientFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.javatuples.Triplet;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ServingApiKeysMigration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(ServingApiKeysMigration.class);
  
  protected Connection connection;
  private KubernetesClient kubeClient;
  private boolean dryRun;
  private ExpatVariablesFacade expatVariablesFacade;
  private SecureRandom secureRandom;
  
  private final static String UPDATE_API_KEY_SCOPES = "UPDATE api_key_scope SET `scope` = ? WHERE `scope` = ?";
  private final static String GET_ACTIVATED_USERS = "SELECT uid, username, email FROM users WHERE " +
    "email NOT IN ('serving@hopsworks.se', 'agent@hops.io', 'onlinefs@hopsworks.ai') AND " +
    "status = 2";
  private final static String GET_PROJECTS_BY_USER = "SELECT projectname FROM project WHERE id IN (SELECT project_id " +
    "FROM project_team WHERE team_member = ?)";
  private final static String GET_API_KEY_BY_PREFIX = "SELECT id FROM api_key WHERE prefix = ?";
  private final static String GET_API_KEY_BY_NAME = "SELECT id FROM api_key WHERE `name` = ? AND user_id = ? AND " +
    "reserved = 1";
  private final static String GET_API_KEY_WITH_SERVING_BY_USER = "SELECT prefix, secret, salt, created, `name` FROM " +
    "api_key WHERE user_id = ? AND reserved = 0 AND id IN (SELECT api_key FROM api_key_scope WHERE `scope` = " +
    "'SERVING')";
  private final static String INSERT_API_KEY = "REPLACE INTO api_key (prefix, secret, salt, created, " +
    "modified, `name`, user_id, reserved) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
  private final static String INSERT_API_KEY_SCOPE = "REPLACE INTO api_key_scope (api_key, `scope`) VALUES (?, ?)";
  private final static String DELETE_API_KEY_SCOPES = "DELETE FROM api_key_scope WHERE api_key = ?";
  private final static String DELETE_API_KEYS = "DELETE FROM api_key WHERE id = ?";
  
  private final String SERVING_API_KEY_NAME = "serving";
  private final String[] SERVING_API_KEY_NAME_SCOPES =
    new String[]{"SERVING", "DATASET_VIEW", "KAFKA", "PROJECT", "FEATURESTORE"};
  private final int RANDOM_KEY_LEN = 64;
  private final int RANDOM_PREFIX_KEY_LEN = 16;
  
  private final static String HOPS_SYSTEM_NAMESPACE = "hops-system";
  
  private final static String LABEL_PREFIX = "serving.hops.works";
  private final static String API_KEY_NAME_LABEL_NAME = LABEL_PREFIX + "/name";
  private final static String API_KEY_RESERVED_LABEL_NAME = LABEL_PREFIX + "/reserved";
  private final static String API_KEY_SCOPE_LABEL_NAME = LABEL_PREFIX + "/scope";
  private final static String API_KEY_USER_LABEL_NAME = LABEL_PREFIX + "/user";
  private final static String API_KEY_MODIFIED_LABEL_NAME = LABEL_PREFIX + "/modified";
  
  private final static String API_KEY_SECRET_KEY = "secret";
  private final static String API_KEY_SALT_KEY = "salt";
  private final static String API_KEY_USER_KEY = "user";
  
  private final static String SERVING_API_KEY_SECRET_KEY = "apiKey";
  private final static String SERVING_API_KEY_SECRET_SUFFIX = "--serving";
  private final static String SERVING_API_KEY_SECRET_PREFIX = "api-key";
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting serving api keys migration");
    
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    
    PreparedStatement updateApiKeyScopesStmt = null;
    PreparedStatement getActivatedUsersStmt = null;
    PreparedStatement insertServingApiKeyStmt = null;
    PreparedStatement insertServingApiKeyScopesStmt = null;
    PreparedStatement getProjectsByUserStmt = null;
    PreparedStatement getApiKeyByPrefixStmt = null;
    PreparedStatement getApiKeyWithServingByUserStmt = null;
    PreparedStatement deleteApiKeyScopesStmt = null;
    PreparedStatement deleteApiKeysStmt = null;
    PreparedStatement getApiKeyByNameStmt = null;
    try {
      connection.setAutoCommit(false);
      
      // migrate api keys with scope INFERENCE to SERVING
      updateApiKeyScopesStmt = connection.prepareStatement(UPDATE_API_KEY_SCOPES);
      updateApiKeyScopesStmt.setString(1, "SERVING");
      updateApiKeyScopesStmt.setString(2, "INFERENCE");
      if (dryRun) {
        LOGGER.info(updateApiKeyScopesStmt.toString());
      } else {
        updateApiKeyScopesStmt.executeUpdate();
      }
      
      // create serving api keys for existing users
      boolean isKFServingInstalled;
      try {
        // -- check kubernetes is installed
        ExpatVariables kfservingInstalled = expatVariablesFacade.findById("kube_kfserving_installed");
        isKFServingInstalled = Boolean.parseBoolean(kfservingInstalled.getValue());
      } catch (IllegalAccessException | SQLException | InstantiationException ex) {
        String errorMsg = "Could not migrate serving api keys";
        LOGGER.error(errorMsg);
        throw new MigrationException(errorMsg, ex);
      }
      
      if (isKFServingInstalled) {
        try {
          kubeClient = KubernetesClientFactory.getClient();
        } catch (ConfigurationException e) {
          throw new MigrationException("Cannot read kube client configuration", e);
        }

        insertServingApiKeyStmt = connection.prepareStatement(INSERT_API_KEY, Statement.RETURN_GENERATED_KEYS);
        insertServingApiKeyScopesStmt = connection.prepareStatement(INSERT_API_KEY_SCOPE);
        getApiKeyByPrefixStmt = connection.prepareStatement(GET_API_KEY_BY_PREFIX);
        deleteApiKeyScopesStmt = connection.prepareStatement(DELETE_API_KEY_SCOPES);
        deleteApiKeysStmt = connection.prepareStatement(DELETE_API_KEYS);
        getApiKeyByNameStmt = connection.prepareStatement(GET_API_KEY_BY_NAME);
        
        // -- per activated user
        getActivatedUsersStmt = connection.prepareStatement(GET_ACTIVATED_USERS);
        ResultSet activatedUsersResultSet = getActivatedUsersStmt.executeQuery();
        while (activatedUsersResultSet.next()) {
          int uid = activatedUsersResultSet.getInt(1);
          String username = activatedUsersResultSet.getString(2);
          String email = activatedUsersResultSet.getString(3);
          
          // -- per user's api key with serving scope
          getApiKeyWithServingByUserStmt = connection.prepareStatement(GET_API_KEY_WITH_SERVING_BY_USER);
          getApiKeyWithServingByUserStmt.setInt(1, uid);
          createKubeApiKeySecrets(username, getApiKeyWithServingByUserStmt);
          getApiKeyWithServingByUserStmt.close();
          
          // -- create serving api key
          String name = getServingApiKeyName(username, uid);
          Date date = new Date();
          java.sql.Date sqlDate = new java.sql.Date(date.getTime());
          Triplet<String, String, String> secret = generateApiKey(getApiKeyByPrefixStmt);
          String hash = DigestUtils.sha256Hex(secret.getValue1() + secret.getValue2());
          
          // -- delete api key and api key scopes if it exists
          // api_key can't be updated (ON UPDATE NO ACTION),to ensure idempotence first we delete possible existing keys
          deleteServingApiKey(uid, name, getApiKeyByNameStmt, deleteApiKeyScopesStmt, deleteApiKeysStmt, false);
          
          // -- insert api key and api key scope
          insertServingApiKey(uid, name, secret, hash, sqlDate, insertServingApiKeyStmt, insertServingApiKeyScopesStmt);
          
          // -- create kube secrets
          getProjectsByUserStmt = connection.prepareStatement(GET_PROJECTS_BY_USER);
          getProjectsByUserStmt.setString(1, email);
          createKubeServingApiKeySecrets(name, secret, hash, username, date, getProjectsByUserStmt);
          getProjectsByUserStmt.close();
        }
        activatedUsersResultSet.close();
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch (IllegalStateException | SQLException ex) {
      String errorMsg = "Could not migrate serving api keys";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      closeConnections(updateApiKeyScopesStmt, getActivatedUsersStmt, insertServingApiKeyStmt,
        insertServingApiKeyScopesStmt, getProjectsByUserStmt, getApiKeyByPrefixStmt, getApiKeyWithServingByUserStmt,
        deleteApiKeyScopesStmt, deleteApiKeysStmt, getApiKeyByNameStmt);
      if (kubeClient != null) { kubeClient.close(); }
    }
    LOGGER.info("Finished serving api keys migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting serving api keys rollback");
    
    try {
      setup();
    } catch (SQLException | ConfigurationException ex) {
      String errorMsg = "Could not initialize database connection";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }
  
    PreparedStatement updateApiKeyScopesStmt = null;
    PreparedStatement deleteApiKeyScopesStmt = null;
    PreparedStatement deleteApiKeysStmt = null;
    PreparedStatement getActivatedUsersStmt = null;
    PreparedStatement getApiKeyByNameStmt = null;
    try {
      connection.setAutoCommit(false);
      
      // migrate api keys with scope SERVING to INFERENCE
      updateApiKeyScopesStmt = connection.prepareStatement(UPDATE_API_KEY_SCOPES);
      updateApiKeyScopesStmt.setString(1, "INFERENCE");
      updateApiKeyScopesStmt.setString(2, "SERVING");
      if (dryRun) {
        LOGGER.info(updateApiKeyScopesStmt.toString());
      } else {
        updateApiKeyScopesStmt.executeUpdate();
      }
      
      // delete serving api keys for existing users
      boolean isKFServingInstalled;
      try {
        // -- check kubernetes is installed
        ExpatVariables kfservingInstalled = expatVariablesFacade.findById("kube_kfserving_installed");
        isKFServingInstalled = Boolean.parseBoolean(kfservingInstalled.getValue());
      } catch (IllegalAccessException | SQLException | InstantiationException ex) {
        String errorMsg = "Could not rollback serving api keys";
        LOGGER.error(errorMsg);
        throw new RollbackException(errorMsg, ex);
      }
      
      if (isKFServingInstalled) {
        try {
          kubeClient = KubernetesClientFactory.getClient();
        } catch (ConfigurationException e) {
          throw new RollbackException("Cannot read kube client configuration", e);
        }
        
        deleteApiKeysStmt = connection.prepareStatement(DELETE_API_KEYS);
        deleteApiKeyScopesStmt = connection.prepareStatement(DELETE_API_KEY_SCOPES);
        getApiKeyByNameStmt = connection.prepareStatement(GET_API_KEY_BY_NAME);
        
        // -- per activated user
        getActivatedUsersStmt = connection.prepareStatement(GET_ACTIVATED_USERS);
        ResultSet activatedUsersResultSet = getActivatedUsersStmt.executeQuery();
        while (activatedUsersResultSet.next()) {
          int uid = activatedUsersResultSet.getInt(1);
          String username = activatedUsersResultSet.getString(2);
          
          // -- delete serving api key and scopes
          String name = getServingApiKeyName(username, uid);
          deleteServingApiKey(uid, name, getApiKeyByNameStmt, deleteApiKeyScopesStmt, deleteApiKeysStmt, true);
          
          // -- delete all kube serving api key secrets
          deleteKubeSecrets(username);
        }
        activatedUsersResultSet.close();
        if (dryRun) {
          LOGGER.info(deleteApiKeyScopesStmt.toString());
          LOGGER.info(deleteApiKeysStmt.toString());
        } else {
          deleteApiKeyScopesStmt.executeBatch();
          deleteApiKeysStmt.executeBatch();
        }
      }
      
      connection.commit();
      connection.setAutoCommit(true);
    } catch (IllegalStateException | SQLException ex) {
      String errorMsg = "Could not rollback serving api keys";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      closeConnections(updateApiKeyScopesStmt, deleteApiKeyScopesStmt, deleteApiKeysStmt,
        getActivatedUsersStmt, getApiKeyByNameStmt);
      kubeClient.close();
    }
    LOGGER.info("Finished serving api keys rollback");
  }
  
  private void insertServingApiKey(int uid, String name, Triplet<String, String, String> secret,
    String hash, java.sql.Date date, PreparedStatement insertServingApiKeyStmt,
    PreparedStatement insertServingApiKeyScopesStmt) throws SQLException {
    
    insertServingApiKeyStmt.setString(1, secret.getValue0());
    insertServingApiKeyStmt.setString(2, hash);
    insertServingApiKeyStmt.setString(3, secret.getValue2());
    insertServingApiKeyStmt.setDate(4, date);
    insertServingApiKeyStmt.setDate(5, date);
    insertServingApiKeyStmt.setString(6, name);
    insertServingApiKeyStmt.setInt(7, uid);
    insertServingApiKeyStmt.setInt(8, 1);
    if (dryRun) {
      LOGGER.info(insertServingApiKeyStmt.toString());
      return; // nothing to do below
    } else {
      insertServingApiKeyStmt.executeUpdate();
    }
    ResultSet rs = insertServingApiKeyStmt.getGeneratedKeys();
    rs.next();
    int id = rs.getInt(1);
    rs.close();
    
    for (String scope : SERVING_API_KEY_NAME_SCOPES) {
      insertServingApiKeyScopesStmt.setInt(1, id);
      insertServingApiKeyScopesStmt.setString(2, scope);
      insertServingApiKeyScopesStmt.addBatch();
    }
    insertServingApiKeyScopesStmt.executeBatch();
    
    LOGGER.info("Serving api key " + name + " created for user " + uid);
  }
  
  private void deleteServingApiKey(int uid, String name, PreparedStatement getApiKeyByNameStmt,
    PreparedStatement deleteApiKeyScopesStmt, PreparedStatement deleteApiKeysStmt, boolean batch) throws SQLException {
    getApiKeyByNameStmt.setString(1, name);
    getApiKeyByNameStmt.setInt(2, uid);
    ResultSet apiKeyResultSet = getApiKeyByNameStmt.executeQuery();
    if (!apiKeyResultSet.next()) {
      apiKeyResultSet.close();
      return;  // no serving api key for this user
    }
    
    int apiKeyId = apiKeyResultSet.getInt(1);
    apiKeyResultSet.close();
    
    deleteApiKeyScopesStmt.setInt(1, apiKeyId);
    deleteApiKeysStmt.setInt(1, apiKeyId);
    
    String msg;
    if (batch) {
      deleteApiKeyScopesStmt.addBatch();
      deleteApiKeysStmt.addBatch();
      msg = " queued for removal";
    } else {
      deleteApiKeyScopesStmt.execute();
      deleteApiKeysStmt.execute();
      msg = " removed";
    }
    LOGGER.info("Serving api key " + name + " of user " + uid + msg);
  }
  
  private void deleteKubeSecrets(String username) {
    Map<String, String> labels = getApiKeySecretLabels(null, null, username, null);
    if (dryRun) {
      LOGGER.info("Delete secrets in any namespace with labels: " + labels.entrySet().stream().map(e -> e.getKey() +
        ":" + e.getValue()).collect(Collectors.joining(",")));
    } else {
      LOGGER.info("Delete secrets in any namespace with labels: " + labels.entrySet().stream().map(e -> e.getKey() +
        ":" + e.getValue()).collect(Collectors.joining(",")));
      kubeClient.secrets().inAnyNamespace().withLabels(labels).delete();
    }
    LOGGER.info("Serving api key secrets removed in any namespace for user: " + username);
  }
  
  private void createKubeServingApiKeySecrets(String name, Triplet<String, String, String> secret, String hash,
    String username, Date date, PreparedStatement getProjectsByUserStmt) throws SQLException {
    
    // -- create kube secret in hops-system
    String secretName = getServingApiKeySecretName(secret.getValue0());
    Map<String, String> labels = getApiKeySecretLabels(true, name, username, date);
    Map<String, byte[]> data = getApiKeySecretData(secret.getValue2(), hash, username, getApiKey(secret));
    createKubeSecret(HOPS_SYSTEM_NAMESPACE, secretName, data, labels);
    LOGGER.info("Serving api key secret created for user " + username + " in hops-system");
    
    // -- per user's project
    ResultSet projectsResultSet = getProjectsByUserStmt.executeQuery();
    while (projectsResultSet.next()) {
      String projectName = projectsResultSet.getString(1);
      // -- create kube secret in project-namespace
      String namespace = projectName.toLowerCase().replaceAll("[^a-z0-9-]", "-");
      secretName = getProjectServingApiKeySecretName(username);
      data = new HashMap<>();
      data.put(SERVING_API_KEY_SECRET_KEY, getApiKey(secret).getBytes());
      createKubeSecret(namespace, secretName, data, labels);
      LOGGER.info("Serving api key secret created for user " + username + " in project " + projectName);
    }
    projectsResultSet.close();
  }
  
  private void createKubeApiKeySecrets(String username, PreparedStatement getApiKeyWithServingStmt)
      throws SQLException {
    ResultSet apiKeyWithServing = getApiKeyWithServingStmt.executeQuery();
    while (apiKeyWithServing.next()) {
      String prefix = apiKeyWithServing.getString(1);
      String hash = apiKeyWithServing.getString(2);
      String salt = apiKeyWithServing.getString(3);
      java.sql.Date date = apiKeyWithServing.getDate(4);
      String name = apiKeyWithServing.getString(5);
      
      String secretName = getServingApiKeySecretName(prefix);
      Map<String, String> labels = getApiKeySecretLabels(false, name, username, date);
      Map<String, byte[]> data = getApiKeySecretData(salt, hash, username, null);
      createKubeSecret(HOPS_SYSTEM_NAMESPACE, secretName, data, labels);
      LOGGER.info("Personal api key secret created for user " + username + " with name " + secretName);
    }
    apiKeyWithServing.close();
  }
  
  private void createKubeSecret(String namespace, String name, Map<String, byte[]> filenameToContent, Map<String,
    String> labels) throws KubernetesClientException {
    Secret secret = new SecretBuilder()
      .withMetadata(
        new ObjectMetaBuilder()
          .withName(name)
          .withLabels(labels)
          .build())
      .withData(filenameToContent.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> Base64.getEncoder().encodeToString(e.getValue()))))
      .build();
    if (dryRun) {
      LOGGER.info("Create secret with name " + name + " in namespace: " + namespace);
    } else {
      kubeClient.secrets().inNamespace(namespace).createOrReplace(secret);
    }
  }
  
  private String getServingApiKeyName(String username, int uid) {
    return SERVING_API_KEY_NAME + "_" + username + "_" + uid;
  }
  
  private String getServingApiKeySecretName(String apiKeyPrefix) {
    StringBuilder prefixWithMask = new StringBuilder("api-key-" + apiKeyPrefix.toLowerCase() + "-");
    for (char c : apiKeyPrefix.toCharArray()) {
      prefixWithMask.append(Character.isUpperCase(c) ? '1' : '0');
    }
    return prefixWithMask.toString();
  }
  
  private String getProjectServingApiKeySecretName(String username) {
    return SERVING_API_KEY_SECRET_PREFIX + "-" + username + SERVING_API_KEY_SECRET_SUFFIX;
  }
  
  private Map<String, String> getApiKeySecretLabels(Boolean reserved, String apiKeyName, String username,
    Date modified) {
    return new HashMap<String, String>() {
      {
        put(API_KEY_SCOPE_LABEL_NAME, SERVING_API_KEY_NAME); // serving
        if (reserved != null) {
          put(API_KEY_RESERVED_LABEL_NAME, String.valueOf(reserved));
        }
        if (apiKeyName != null) {
          put(API_KEY_NAME_LABEL_NAME, apiKeyName);
        }
        if (username != null) {
          put(API_KEY_USER_LABEL_NAME, username);
        }
        if (modified != null) {
          put(API_KEY_MODIFIED_LABEL_NAME, String.valueOf(modified.getTime()));
        }
      }
    };
  }
  
  private Map<String, byte[]> getApiKeySecretData(String salt, String hash, String username, String apiKey) {
    return new HashMap<String, byte[]>() {
      {
        put(API_KEY_SECRET_KEY, hash.getBytes());
        put(API_KEY_SALT_KEY, salt.getBytes());
        put(API_KEY_USER_KEY, username.getBytes());
        if (apiKey != null) {
          put(SERVING_API_KEY_SECRET_KEY, apiKey.getBytes());
        }
      }
    };
  }
  
  private String getApiKey(Triplet<String, String, String> secret) {
    return secret.getValue0() + "." + secret.getValue1();
  }
  
  private String generateRandomString(int length) {
    return RandomStringUtils.random(length, 0, 0, true, true, null, secureRandom);
  }
  
  private String generateSecureRandomString(int length) {
    byte[] bytes = new byte[length];
    secureRandom.nextBytes(bytes);
    byte[] encoded = Base64.getEncoder().encode(bytes);
    return new String(encoded, StandardCharsets.UTF_8);
  }
  
  private Triplet<String, String, String> generateApiKey(PreparedStatement getApiKeyByPrefixStmt)
    throws MigrationException, SQLException {
    int retry = 10;
    String id = generateRandomString(RANDOM_PREFIX_KEY_LEN);
    String secret = generateRandomString(RANDOM_KEY_LEN);
    String salt = generateSecureRandomString(RANDOM_KEY_LEN);
    
    while (apiKeyWithPrefixExists(id, getApiKeyByPrefixStmt) && (retry-- > 0)) {
      id = generateRandomString(RANDOM_PREFIX_KEY_LEN);
      secret = generateRandomString(RANDOM_KEY_LEN);
      salt = generateSecureRandomString(RANDOM_KEY_LEN);
    }
    if (retry < 1) {
      String errorMsg = "Could not migrate serving api keys";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg);
    }
    return new Triplet<>(id, secret, salt);
  }
  
  private boolean apiKeyWithPrefixExists(String prefix, PreparedStatement getApiKeyByPrefixStmt) throws SQLException {
    getApiKeyByPrefixStmt.setString(1, prefix);
    ResultSet apiKeyResultSet = getApiKeyByPrefixStmt.executeQuery();
    boolean exists = apiKeyResultSet.next();
    apiKeyResultSet.close();
    return exists;
  }
  
  private void setup() throws SQLException, ConfigurationException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    connection = DbConnectionFactory.getConnection();
    expatVariablesFacade = new ExpatVariablesFacade(ExpatVariables.class, connection);
    secureRandom = new SecureRandom();
  }
  
  private void closeConnections(PreparedStatement... stmts) {
    try {
      for (PreparedStatement stmt : stmts) {
        if (stmt != null) {
          stmt.close();
        }
      }
    } catch (SQLException ex) {
      //do nothing
    }
  }
}

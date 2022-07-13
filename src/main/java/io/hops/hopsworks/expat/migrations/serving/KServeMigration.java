/**
 * This file is part of Expat
 * Copyright (C) 2022, Logical Clocks AB. All rights reserved
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

import io.fabric8.kubernetes.api.model.batch.DoneableJob;
import io.fabric8.kubernetes.api.model.batch.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import io.hops.hopsworks.expat.configuration.ConfigurationBuilder;
import io.hops.hopsworks.expat.configuration.ExpatConf;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariables;
import io.hops.hopsworks.expat.db.dao.util.ExpatVariablesFacade;
import io.hops.hopsworks.expat.kubernetes.KubernetesClientFactory;
import io.hops.hopsworks.expat.migrations.MigrateStep;
import io.hops.hopsworks.expat.migrations.MigrationException;
import io.hops.hopsworks.expat.migrations.RollbackException;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

public class KServeMigration implements MigrateStep {
  private static final Logger LOGGER = LogManager.getLogger(KServeMigration.class);
  
  protected Connection connection;
  private KubernetesClient kubeClient;
  private boolean dryRun;
  private ExpatVariablesFacade expatVariablesFacade;
  
  @Override
  public void migrate() throws MigrationException {
    LOGGER.info("Starting kserve migration");
  
    try {
      setup();
    } catch (ConfigurationException ex) {
      String errorMsg = "Could not get expat configuration";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    }
    
    try {
      boolean isKServeInstalled;
      try {
        // -- check kfserving is installed. At this point, the Hopsworks variable has been updated but KFServing is
        // still installed.
        ExpatVariables kserveInstalled = expatVariablesFacade.findById("kube_kserve_installed");
        isKServeInstalled = Boolean.parseBoolean(kserveInstalled.getValue());
      } catch (IllegalAccessException | SQLException | InstantiationException ex) {
        String errorMsg = "Could not migrate to kserve";
        LOGGER.error(errorMsg);
        throw new MigrationException(errorMsg, ex);
      }
      
      if (isKServeInstalled) {
        try {
          kubeClient = KubernetesClientFactory.getClient();
        } catch (ConfigurationException e) {
          throw new MigrationException("Cannot read kube client configuration", e);
        }
        
        // Apply kserve job migration. No need to stop servings, the migration job takes care of upgrading existing
        // inference services.
        Configuration config = ConfigurationBuilder.getConfiguration();
        Path migrationJobYaml = Paths.get(config.getString(ExpatConf.EXPAT_PATH), "bin", "kserve-migration-job.yaml");
        Job migrationJob = kubeClient.batch().jobs().load(new FileInputStream(migrationJobYaml.toString())).get();
        migrationJob = kubeClient.batch().jobs().inNamespace("kserve").create(migrationJob);
  
        // Wait for migration job to complete
        // Get All pods created by the job
        Job myJob = client.extensions().jobs()
          .load(new FileInputStream("/path/x.yaml"))
          .create();
        boolean jobActive = true;
        while(jobActive){
          myJob = client.extensions().jobs()
            .inNamespace(myJob.getMetadata().getNamespace())
            .withName(myJob.getMetadata().getName())
            .get();
          JobStatus myJobStatus = myJob.getStatus();
          System.out.println("==================");
          System.out.println(myJobStatus.toString());
    
          if(myJob.getStatus().getActive()==null){
            jobActive = false;
          }
          else {
            System.out.println(myJob.getStatus().getActive());
            System.out.println("Sleeping for a minute before polling again!!");
            Thread.sleep(60000);
          }
        }
        
        
        PodList podList =
          kubeClient.pods().inNamespace("kserve").withLabel("job-name", job.getMetadata().getName()).list();
        // Wait for pod to complete
        client.pods().inNamespace(namespace).withName(podList.getItems().get(0).getMetadata().getName())
          .waitUntilCondition(pod -> pod.getStatus().getPhase().equals("Succeeded"), 2, TimeUnit.MINUTES);
        
        // Delete resources
        /*
          kubectl delete ClusterRoleBinding cluster-migration-rolebinding
          kubectl delete ClusterRole cluster-migration-role
          kubectl delete ServiceAccount cluster-migration-svcaccount -n kserve
        */
      }
    } catch (IllegalStateException | SQLException | FileNotFoundException ex) {
      String errorMsg = "Could not migrate to kserve";
      LOGGER.error(errorMsg);
      throw new MigrationException(errorMsg, ex);
    } finally {
      if (kubeClient != null) { kubeClient.close(); }
    }
    LOGGER.info("Finished kserve migration");
  }
  
  @Override
  public void rollback() throws RollbackException {
    LOGGER.info("Starting kserve rollback");
  
    try {
      setup();
    } catch (ConfigurationException ex) {
      String errorMsg = "Could not get expat configuration";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    }
    
    try {
      boolean isKServeInstalled;
      try {
        // -- check kserve is installed.
        ExpatVariables kserveInstalled = expatVariablesFacade.findById("kube_kserve_installed");
        isKServeInstalled = Boolean.parseBoolean(kserveInstalled.getValue());
      } catch (IllegalAccessException | SQLException | InstantiationException ex) {
        String errorMsg = "Could not rollback kserve";
        LOGGER.error(errorMsg);
        throw new RollbackException(errorMsg, ex);
      }
      
      if (isKServeInstalled) {
        try {
          kubeClient = KubernetesClientFactory.getClient();
        } catch (ConfigurationException e) {
          throw new RollbackException("Cannot read kube client configuration", e);
        }
        
        // Stop servings
        
        // Remove KServe CRDs and namespace.
        
        
      }
    } catch (IllegalStateException | SQLException ex) {
      String errorMsg = "Could not rollback kserve";
      LOGGER.error(errorMsg);
      throw new RollbackException(errorMsg, ex);
    } finally {
      if (kubeClient != null) { kubeClient.close(); }
    }
    LOGGER.info("Finished kserve rollback");
  }
  
  private void setup() throws ConfigurationException {
    Configuration conf = ConfigurationBuilder.getConfiguration();
    dryRun = conf.getBoolean(ExpatConf.DRY_RUN);
    expatVariablesFacade = new ExpatVariablesFacade(ExpatVariables.class, connection);
  }
}

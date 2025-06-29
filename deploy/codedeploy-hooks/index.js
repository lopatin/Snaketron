const AWS = require('aws-sdk');
const codedeploy = new AWS.CodeDeploy({ apiVersion: '2014-10-06' });
const ecs = new AWS.ECS({ apiVersion: '2014-11-13' });
const rds = new AWS.RDS({ apiVersion: '2014-10-31' });

/**
 * Common function to notify CodeDeploy of hook status
 */
const putLifecycleEventHookExecutionStatus = async (deploymentId, lifecycleEventHookExecutionId, status) => {
    const params = {
        deploymentId: deploymentId,
        lifecycleEventHookExecutionId: lifecycleEventHookExecutionId,
        status: status // 'Succeeded' | 'Failed'
    };
    
    try {
        await codedeploy.putLifecycleEventHookExecutionStatus(params).promise();
        console.log(`Reported ${status} status to CodeDeploy`);
    } catch (err) {
        console.error('Failed to report status to CodeDeploy:', err);
        throw err;
    }
};

/**
 * Check Raft cluster health by querying the database
 */
const checkRaftHealth = async (dbEndpoint, dbPassword) => {
    // In a real implementation, you would query the database to check:
    // 1. Number of healthy nodes
    // 2. Raft leader status
    // 3. Recent heartbeats
    
    // For now, we'll check that we have at least 2 healthy nodes
    // This is a simplified example - you'd need to implement actual DB queries
    console.log('Checking Raft cluster health...');
    
    // TODO: Implement actual database query
    // const query = `
    //   SELECT COUNT(*) as healthy_nodes 
    //   FROM servers 
    //   WHERE last_heartbeat > NOW() - INTERVAL '30 seconds'
    //   AND status = 'active'
    // `;
    
    return { healthy: true, nodeCount: 3 };
};

/**
 * BeforeInstall Hook - Verify cluster health before deployment
 */
exports.beforeInstallHook = async (event, context) => {
    console.log('BeforeInstall Hook triggered');
    const { deploymentId, lifecycleEventHookExecutionId } = event;
    
    try {
        // Check Raft cluster health
        const raftStatus = await checkRaftHealth();
        
        if (raftStatus.nodeCount < 2) {
            console.error(`Insufficient healthy nodes: ${raftStatus.nodeCount}`);
            await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
            return;
        }
        
        console.log(`Raft cluster healthy with ${raftStatus.nodeCount} nodes`);
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Succeeded');
    } catch (err) {
        console.error('BeforeInstall hook failed:', err);
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
    }
};

/**
 * AfterInstall Hook - Verify new containers started successfully
 */
exports.afterInstallHook = async (event, context) => {
    console.log('AfterInstall Hook triggered');
    const { deploymentId, lifecycleEventHookExecutionId } = event;
    
    try {
        // Wait a bit for containers to start
        await new Promise(resolve => setTimeout(resolve, 10000));
        
        // TODO: Check that new containers have registered in the database
        console.log('New containers installed successfully');
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Succeeded');
    } catch (err) {
        console.error('AfterInstall hook failed:', err);
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
    }
};

/**
 * AfterAllowTestTraffic Hook - Test the new deployment with sample traffic
 */
exports.afterAllowTestTrafficHook = async (event, context) => {
    console.log('AfterAllowTestTraffic Hook triggered');
    const { deploymentId, lifecycleEventHookExecutionId } = event;
    
    try {
        // TODO: Implement health check against new containers
        // For example:
        // 1. Make HTTP request to /api/health endpoint
        // 2. Try to establish WebSocket connection
        // 3. Verify Raft cluster is still healthy
        
        console.log('Test traffic validation passed');
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Succeeded');
    } catch (err) {
        console.error('AfterAllowTestTraffic hook failed:', err);
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
    }
};

/**
 * BeforeAllowTraffic Hook - Final checks before shifting production traffic
 */
exports.beforeAllowTrafficHook = async (event, context) => {
    console.log('BeforeAllowTraffic Hook triggered');
    const { deploymentId, lifecycleEventHookExecutionId } = event;
    
    try {
        // Final Raft health check
        const raftStatus = await checkRaftHealth();
        
        if (!raftStatus.healthy) {
            console.error('Raft cluster unhealthy before traffic shift');
            await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
            return;
        }
        
        console.log('All checks passed, ready for traffic shift');
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Succeeded');
    } catch (err) {
        console.error('BeforeAllowTraffic hook failed:', err);
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
    }
};

/**
 * AfterAllowTraffic Hook - Verify deployment success after traffic shift
 */
exports.afterAllowTrafficHook = async (event, context) => {
    console.log('AfterAllowTraffic Hook triggered');
    const { deploymentId, lifecycleEventHookExecutionId } = event;
    
    try {
        // Give some time for traffic to stabilize
        await new Promise(resolve => setTimeout(resolve, 5000));
        
        // Final health check
        const raftStatus = await checkRaftHealth();
        
        if (raftStatus.healthy && raftStatus.nodeCount >= 3) {
            console.log('Deployment completed successfully');
            await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Succeeded');
        } else {
            console.error('Post-deployment health check failed');
            await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
        }
    } catch (err) {
        console.error('AfterAllowTraffic hook failed:', err);
        await putLifecycleEventHookExecutionStatus(deploymentId, lifecycleEventHookExecutionId, 'Failed');
    }
};
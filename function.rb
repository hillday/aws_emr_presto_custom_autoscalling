require 'json'
require 'presto/metrics'
require 'logger'
require 'aws-sdk-emr'
require 'aws-sdk-cloudwatch'
require 'httparty'

$g_match_name = ENV['MATCH_NAME']
$g_port = ENV['PRESTO_PORT']
$g_region = ENV['REGION']

$g_logger = Logger.new(STDOUT)


def load_metrics()
    $g_logger.info('## load metrics json from file...')
    metrics_json = File.read('metrics.json')
    $g_logger.info('## load metrics json %p' % metrics_json)
    metrics_obj = JSON.parse(metrics_json)
    return metrics_obj
end
        

def get_emr_clusters()
    $g_logger.info('## get emr clusters...')
    client = Aws::EMR::Client.new()
    clusters = client.list_clusters({
      cluster_states: ["RUNNING","WAITING"],
    })
    
    simple_clusters = Array.new
    clusters['clusters'].each do |cluster|
        # puts cluster
        cluster_id = cluster.id
        cluster_name = cluster.name
        #puts "#{cluster_id},#{cluster_name}"
        if cluster_name.include?($g_match_name) then
            cluster_master = client.list_instances({
                  cluster_id: cluster_id, # required
                  instance_group_types: ["MASTER"], # accepts MASTER, CORE, TASK
                })
            cluster_master_ip = cluster_master['instances'][0].private_ip_address
            cluster_simple_info = Hash["id" => cluster_id, "name" => cluster_name, "master_ip"=> cluster_master_ip]
            $g_logger.info('## append simple cluster info %p' % cluster_simple_info)
            simple_clusters.append(cluster_simple_info)
            #puts cluster_simple_info
        end
    end
    
    return simple_clusters
end

def get_presto_metrics(cluster,metrics)
    $g_logger.info('## query presto metrics...')
    
    query_metrics = Array.new
    node_mettrics_name = ''
    metrics.each do |item|
        if item['presto_metrics_type'] == 'query' then
            query_metrics.append(item['presto_name'])
        end
        if item['presto_metrics_type'] == 'node' then
            node_mettrics_name = item['presto_name']
        end
    end
     
    client = Presto::Metrics::Client.new(:host => cluster['master_ip'], :port=>$g_port)

    results = client.query_manager_metrics(query_metrics)
    if node_mettrics_name != '' then
        results[node_mettrics_name] = client.node_metrics.size
    end
    $g_logger.info('## get presto metrics %p' % results)
    return results
end


def put_metrics_to_cloudwatch(cluster_id,name,unit,value)
    $g_logger.info('## put metrics to cloudwatch...')
    client = Aws::CloudWatch::Client.new(:region => $g_region)
    resp = client.put_metric_data({
      namespace: "AWS/ElasticMapReduce", # required
      metric_data: [ # required
        {
          metric_name: name, # required
          dimensions: [
            {
              name: "JobFlowId", # required
              value: cluster_id, # required
            },
          ],
          timestamp: Time.now,
          value: value,
          unit: unit
        }
      ]
    })
    $g_logger.info('## end put metrics to cloudwatch,reps %p' % resp)
end

def lambda_handler(event:, context:)
    metrics = load_metrics
    # TODO implement
    simple_clusters = get_emr_clusters
    simple_clusters.each do |cluster|
      metric_vals = get_presto_metrics(cluster,metrics)
      metrics.each do |item|
        mv = metric_vals[item['presto_name']]
        if mv == "NaN" then
           mv = 0
        end
        put_metrics_to_cloudwatch(cluster['id'],item['cloudwatch_name'],item['cloudwatch_unit'],mv)
      end
    end
    { statusCode: 200, body: JSON.generate('Hello from Lambda!') }
end

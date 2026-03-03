
#include "cuda_blackboard/cuda_blackboard_subscriber.hpp"

#include "cuda_blackboard/cuda_blackboard.hpp"
#include "cuda_blackboard/negotiated_types.hpp"

#include <chrono>
#include <functional>

namespace cuda_blackboard
{

template <typename T>
CudaBlackboardSubscriber<T>::CudaBlackboardSubscriber(
  rclcpp::Node & node, const std::string & topic_name, [[maybe_unused]] bool add_compatible_sub,
  std::function<void(std::shared_ptr<const T>)> callback)
: node_(node)
{
  using std::placeholders::_1;

  negotiated::NegotiatedSubscriptionOptions negotiation_options;
  negotiation_options.disconnect_on_negotiation_failure = false;

  callback_ = callback;
  negotiated_sub_ = std::make_shared<negotiated::NegotiatedSubscription>(
    node, topic_name + "/cuda", negotiation_options);

  rclcpp::SubscriptionOptions sub_options;
  sub_options.use_intra_process_comm = rclcpp::IntraProcessSetting::Enable;

  negotiated_sub_->add_supported_callback<NegotiationStruct<T>>(
    1.0, rclcpp::QoS(1).durability_volatile(),
    std::bind(&CudaBlackboardSubscriber<T>::instanceIdCallback, this, _1), sub_options);

  std::string ros_type_name = NegotiationStruct<typename T::ros_type>::supported_type_name;

  compatible_sub_ = node.create_subscription<typename T::ros_type>(
    topic_name, rclcpp::SensorDataQoS(),
    std::bind(&CudaBlackboardSubscriber<T>::compatibleCallback, this, _1), sub_options);

  negotiated_sub_->add_compatible_subscription(compatible_sub_, ros_type_name, 0.1);

  negotiated_sub_->start();

  // Create a timer to periodically re-send supported types until negotiation succeeds
  // This fixes the race condition where the publisher may not be ready when we first send
  negotiation_retry_timer_ = node.create_wall_timer(
    std::chrono::milliseconds(500),
    std::bind(&CudaBlackboardSubscriber<T>::retryNegotiationCallback, this));
}

template <typename T>
CudaBlackboardSubscriber<T>::CudaBlackboardSubscriber(
  rclcpp::Node & node, const std::string & topic_name,
  std::function<void(std::shared_ptr<const T>)> callback)
: node_(node)
{
  using std::placeholders::_1;

  negotiated::NegotiatedSubscriptionOptions negotiation_options;
  negotiation_options.disconnect_on_negotiation_failure = false;

  callback_ = callback;
  negotiated_sub_ = std::make_shared<negotiated::NegotiatedSubscription>(
    node, topic_name + "/cuda", negotiation_options);

  rclcpp::SubscriptionOptions sub_options;
  sub_options.use_intra_process_comm = rclcpp::IntraProcessSetting::Enable;

  negotiated_sub_->add_supported_callback<NegotiationStruct<T>>(
    1.0, rclcpp::QoS(1).durability_volatile(),
    std::bind(&CudaBlackboardSubscriber<T>::instanceIdCallback, this, _1), sub_options);

  std::string ros_type_name = NegotiationStruct<typename T::ros_type>::supported_type_name;

  compatible_sub_ = node.create_subscription<typename T::ros_type>(
    topic_name, rclcpp::SensorDataQoS(),
    std::bind(&CudaBlackboardSubscriber<T>::compatibleCallback, this, _1), sub_options);

  negotiated_sub_->add_compatible_subscription(compatible_sub_, ros_type_name, 0.1);

  negotiated_sub_->start();

  // Create a timer to periodically re-send supported types until negotiation succeeds
  // This fixes the race condition where the publisher may not be ready when we first send
  negotiation_retry_timer_ = node.create_wall_timer(
    std::chrono::milliseconds(500),
    std::bind(&CudaBlackboardSubscriber<T>::retryNegotiationCallback, this));
}

template <typename T>
void CudaBlackboardSubscriber<T>::retryNegotiationCallback()
{
  // Safety check - don't do anything if already succeeded
  if (negotiation_succeeded_) {
    if (negotiation_retry_timer_) {
      negotiation_retry_timer_->cancel();
      negotiation_retry_timer_.reset();
    }
    return;
  }

  // Check if negotiation has actually succeeded by checking the negotiated topics info
  // This is only set to success=true when the publisher successfully negotiates
  const auto & topics_info = negotiated_sub_->get_negotiated_topics_info();
  if (topics_info.success && !topics_info.negotiated_topics.empty()) {
    // Negotiation succeeded, stop the timer
    negotiation_succeeded_ = true;
    if (negotiation_retry_timer_) {
      negotiation_retry_timer_->cancel();
      negotiation_retry_timer_.reset();
    }
    RCLCPP_INFO(
      node_.get_logger(),
      "Negotiation succeeded with %zu topics, stopping retry timer",
      topics_info.negotiated_topics.size());
    return;
  }

  // Limit retries to avoid infinite loops (30 retries * 500ms = 15 seconds max)
  if (++negotiation_retry_count_ > 30) {
    RCLCPP_WARN(node_.get_logger(), "Negotiation retry limit reached, stopping timer");
    if (negotiation_retry_timer_) {
      negotiation_retry_timer_->cancel();
      negotiation_retry_timer_.reset();
    }
    return;
  }

  // Log what types we're sending for debugging
  const auto & supported_types = negotiated_sub_->get_supported_types();
  std::string types_str;
  for (const auto & [key, info] : supported_types) {
    types_str += info.supported_type.ros_type_name + "+" +
                 info.supported_type.supported_type_name + " ";
  }
  RCLCPP_INFO(
    node_.get_logger(),
    "Re-sending supported types for negotiation (retry %d): [%s]",
    negotiation_retry_count_,
    types_str.c_str());

  // Re-send supported types to trigger negotiation
  // This is needed because with volatile QoS, the initial message may be lost
  // if the publisher's subscription wasn't ready yet
  negotiated_sub_->start();
}

template <typename T>
void CudaBlackboardSubscriber<T>::instanceIdCallback(const std_msgs::msg::UInt64 & instance_id_msg)
{
  // Stop retry timer if still running - negotiation has succeeded
  if (negotiation_retry_timer_ && !negotiation_succeeded_) {
    negotiation_succeeded_ = true;
    negotiation_retry_timer_->cancel();
    negotiation_retry_timer_.reset();
    RCLCPP_INFO(node_.get_logger(), "Negotiation succeeded (received instance ID), stopping retry timer");
  }

  if (compatible_sub_ && negotiated_sub_->get_negotiated_topic_publisher_count() > 0) {
    const std::string ros_type_name = NegotiationStruct<typename T::ros_type>::supported_type_name;
    negotiated_sub_->remove_compatible_subscription<typename T::ros_type>(
      compatible_sub_, ros_type_name);
    compatible_sub_ = nullptr;

    RCLCPP_INFO(
      node_.get_logger(),
      "A negotiated message has been received, so the compatible callback will be disabled");
  }

  auto & blackboard = CudaBlackboard<T>::getInstance();
  auto data = blackboard.queryData(instance_id_msg.data);
  if (data) {
    callback_(data);
  } else {
    RCLCPP_ERROR_STREAM(
      node_.get_logger(), "There was not data with the requested instance id= "
                            << instance_id_msg.data << " in the blackboard.");
  }
}

template <typename T>
void CudaBlackboardSubscriber<T>::compatibleCallback(
  const std::shared_ptr<const typename T::ros_type> & ros_msg_ptr)
{
  const std::string ros_type_name = NegotiationStruct<typename T::ros_type>::supported_type_name;

  if (compatible_sub_ && negotiated_sub_->get_negotiated_topic_publisher_count() > 0) {
    negotiated_sub_->remove_compatible_subscription<typename T::ros_type>(
      compatible_sub_, ros_type_name);
    compatible_sub_ = nullptr;

    RCLCPP_INFO(
      node_.get_logger(),
      "A negotiation type succeeded, so the compatible callback will be disabled");

    return;
  }

  RCLCPP_WARN_ONCE(
    node_.get_logger(),
    "The compatible callback was called. This results in a performance loss. This behavior is "
    "probably not intended or a temporal measure");
  callback_(std::make_shared<T>(*ros_msg_ptr));
}

}  // namespace cuda_blackboard

template class cuda_blackboard::CudaBlackboardSubscriber<cuda_blackboard::CudaPointCloud2>;
template class cuda_blackboard::CudaBlackboardSubscriber<cuda_blackboard::CudaImage>;

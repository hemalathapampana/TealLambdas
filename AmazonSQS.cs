public virtual Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, CancellationToken cancellationToken = default(CancellationToken))
{
    InvokeOptions invokeOptions = new InvokeOptions();
    invokeOptions.RequestMarshaller = SendMessageRequestMarshaller.Instance;
    invokeOptions.ResponseUnmarshaller = SendMessageResponseUnmarshaller.Instance;
    return InvokeAsync<SendMessageResponse>(request, invokeOptions, cancellationToken);
}
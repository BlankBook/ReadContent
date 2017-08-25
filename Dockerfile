FROM busybox
ADD main /
EXPOSE 80
CMD ["./main"]

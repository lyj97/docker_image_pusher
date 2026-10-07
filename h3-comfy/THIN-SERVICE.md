# H3 thin Pod service image

The H3 source repository is private. Build the thin-service image using `.github/workflows/h3-pod.yml` in `lyj97/h3-service`, with an exact reviewed source revision. This avoids cross-repository source credentials.

The earlier publisher run 37623242726 failed to check out private source and published no image. Its separate workflow has been removed. The existing Comfy base-image workflow is unchanged.

The thin image has no models or production credentials and performs no GPU generation during build. Registry visibility/authentication must be verified before binding a real Pod.

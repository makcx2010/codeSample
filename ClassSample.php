<?php

final class ClassSample
{
    private EntityManagerInterface            $em;
    private ReferrerCategoryRepository        $referrerCategories;
    private ReferrerSubcategoryInfoRepository $referrerSubCategoryInfoRepository;
    private FileManager                       $fileManager;
    private LoggerInterface                   $log;

    public function __construct(
        EntityManagerInterface $em,
        ReferrerCategoryRepository $referrerCategories,
        ReferrerSubcategoryInfoRepository $referrerSubCategoryInfoRepository,
        FileManager $fileManager,
        LoggerInterface $log
    ) {
        $this->em                                = $em;
        $this->referrerCategories                = $referrerCategories;
        $this->referrerSubCategoryInfoRepository = $referrerSubCategoryInfoRepository;
        $this->fileManager                       = $fileManager;
        $this->log                               = $log;
    }

    public function method(Request $request): PageView
    {
        $req          = json_decode($request->getContent(), true);
        $httpReferrer = !empty($req['httpReferrer']) ? $req['httpReferrer'] : null;
        $utmSource    = !empty($req['utmSource']) ? $req['utmSource'] : null;
        $utmMedium    = !empty($req['utmMedium']) ? $req['utmMedium'] : null;
        $utmCampaign  = !empty($req['utmCampaign']) ? $req['utmCampaign'] : null;
        $utmContent   = !empty($req['utmContent']) ? $req['utmContent'] : null;
        $utmTerm      = !empty($req['utmTerm']) ? $req['utmTerm'] : null;

        if (substr($httpReferrer, -1) === '/') $httpReferrer = substr($httpReferrer, 0, -1);
        foreach ([ $_ENV['WWW_DOMAIN'], $_ENV['ADMIN_DOMAIN'] ] as $ownDomain) {
            if (strpos($httpReferrer, $ownDomain) !== false) $httpReferrer = null;
        }

        $parsedUrl = parse_url($httpReferrer);
        $host      = !empty($parsedUrl['host']) ? $parsedUrl['host'] : null;
        $scheme    = $parsedUrl['scheme'] ?? null;

        if (strpos($host, 'www.') === 0) $host = substr($host, 4);

        $view      = new PageView($httpReferrer, $host, $utmSource, $utmMedium, $utmCampaign, $utmContent, $utmTerm);
        $this->em->persist($view);

        if ($host) {
            $subCategoryInfo = $this->referrerSubCategoryInfoRepository->findOneBy([ 'url' => $host ]);
            if (!$subCategoryInfo) {
                if ($scheme && in_array($scheme, [ 'http', 'https' ], true)) {
                    $category  = $this->referrerCategories->findOneBy([ 'type' => ReferrerCategoryType::FROM_SITE() ]);

                    try {
                        $pathToFav = $this->fileManager->uploadSiteFav($host);
                    } catch (\Throwable $e) {
                        $this->log->error('referrer_subcategory:upload:fav', [
                            'message' => $e->getMessage(),
                            'code'    => $e->getCode()
                        ]);
                    }
                } else {
                    $category = $this->referrerCategories->findOneBy([ 'type' => ReferrerCategoryType::MESSENGERS() ]);
                }

                $subCategory = new ReferrerSubcategory($category, $host);
                if (isset($pathToFav)) $subCategory->setIcon($pathToFav);
                $this->em->persist($subCategory);

                $info = new ReferrerSubcategoryInfo($subCategory, $host);
                $this->em->persist($info);
            }
        }
        $this->em->flush();

        return $view;
    }
}
